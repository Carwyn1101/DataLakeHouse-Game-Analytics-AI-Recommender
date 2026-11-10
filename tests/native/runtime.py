"""Bind the reviewed ETL functions to disposable resources and run one scenario."""

from contextlib import contextmanager
import hashlib
import io
import json
import os
from pathlib import Path
import re
import socket
import sys
import tempfile
import time

MINIO_HOST = "fixture-minio"
HMS_HOST = "fixture-hms"
MINIO_ENDPOINT = f"{MINIO_HOST}:9000"
HMS_URI = f"thrift://{HMS_HOST}:9083"
RUNNER_IP = "172.30.233.4"
OBJECTS = {f"bronze/{name}.csv" for name in ("games", "reviews", "popularity")}
SCRIPT_NAMES = {"rawData_to_bronze.py", "bronze_to_silver_etl.py", "silver_to_gold_etl.py"}


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def check_endpoint(host, port, address):
    for _ in range(60):
        resolved = {entry[4][0] for entry in socket.getaddrinfo(host, port, type=socket.SOCK_STREAM)}
        require(resolved == {address}, f"Unexpected DNS address for {host}")
        try:
            with socket.create_connection((address, port), timeout=1, source_address=(RUNNER_IP, 0)) as connection:
                require(connection.getpeername() == (address, port), f"Unexpected TCP peer for {host}")
                require(connection.getsockname()[0] == RUNNER_IP, "Unexpected runner interface")
            return
        except OSError:
            time.sleep(2)
    raise RuntimeError(f"Fixture endpoint is unavailable: {host}")


def preflight(root):
    require(os.environ.get("NATIVE_ALLOW_WRITES") == "1", "Explicit native write opt-in is required")
    manifest = json.loads((root / "run.json").read_bytes())
    project = manifest["project"]
    require(re.fullmatch(r"native-[0-9a-f]{16}", project), "Invalid run identity")
    user = os.environ.get("MINIO_ROOT_USER", "")
    password = os.environ.get("MINIO_ROOT_PASSWORD", "")
    require(user == f"{project}-test", "Unexpected fixture credential identity")
    require(re.fullmatch(r"[0-9a-f]{48}", password), "Unexpected fixture credential shape")
    require(not any(value for key, value in os.environ.items() if key.lower() in
                    {"http_proxy", "https_proxy", "all_proxy"}), "Proxy settings are not allowed")
    routes = Path("/proc/net/route").read_text().splitlines()[1:]
    # The sole non-loopback network must be the internal fixture subnet.
    require(len(routes) == 1 and routes[0].split()[1:4] == ["00E91EAC", "00000000", "0001"]
            and routes[0].split()[7] == "F0FFFFFF", "Unexpected IPv4 routes")
    files = set(manifest["scripts"])
    require(files in (SCRIPT_NAMES, SCRIPT_NAMES | {"bronze_appid.py"}), "Unexpected script set")
    require({path.name for path in (root / "scripts").iterdir()} == files | {"__init__.py"},
            "Unexpected files in script package")
    require((root / "scripts/__init__.py").read_bytes() == b"", "Unexpected package initializer")
    for name, digest in manifest["scripts"].items():
        require(hashlib.sha256((root / "scripts" / name).read_bytes()).hexdigest() == digest,
                "Staged ETL digest mismatch")
    check_endpoint(MINIO_HOST, 9000, "172.30.233.2")
    check_endpoint(HMS_HOST, 9083, "172.30.233.3")
    return manifest, user, password


class ScopedObjects:
    def __init__(self, client, bucket):
        self.client = client
        self.bucket = bucket
        self.puts = 0

    def check(self, bucket, object_name):
        require(bucket == self.bucket and object_name in OBJECTS, "Unexpected Bronze object")

    def get_object(self, bucket_name, object_name):
        self.check(bucket_name, object_name)
        return self.client.get_object(bucket_name, object_name)

    def put_object(self, bucket_name, object_name, data, **kwargs):
        self.check(bucket_name, object_name)
        result = self.client.put_object(bucket_name, object_name, data, **kwargs)
        self.puts += 1
        return result

    def seed(self, object_name, data):
        self.put_object(self.bucket, object_name, io.BytesIO(data), length=len(data))

    def read(self, object_name):
        response = self.get_object(self.bucket, object_name)
        try:
            return response.read()
        finally:
            response.close()
            response.release_conn()


class NativeRun:
    def __init__(self, root, manifest, user, password):
        self.root = root
        self.bucket = manifest["project"]
        self.storage = f"s3a://{self.bucket}"
        self.user = user
        self.password = password

    def new_spark(self, unavailable_catalog=False):
        from pyspark.sql import SparkSession

        warehouse = f"{self.storage}/warehouse"
        # The closed port models HMS unavailability without ever leaving the test network.
        uri = f"thrift://{HMS_HOST}:1" if unavailable_catalog else HMS_URI
        hadoop = {
            "fs.defaultFS": "file:///",
            "fs.s3a.endpoint": f"http://{MINIO_ENDPOINT}",
            "fs.s3a.path.style.access": "true",
            "fs.s3a.connection.ssl.enabled": "false",
            "fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
            "fs.s3a.access.key": self.user,
            "fs.s3a.secret.key": self.password,
            "hive.metastore.uris": uri,
            "hive.metastore.connect.retries": "1",
            "hive.metastore.failure.retries": "0",
            "hive.metastore.client.socket.timeout": "5s",
        }
        settings = {
            "spark.driver.host": RUNNER_IP,
            "spark.driver.bindAddress": RUNNER_IP,
            "spark.ui.enabled": "false",
            "spark.redaction.regex": "(?i)secret|password|token|access[.]key",
            "spark.sql.shuffle.partitions": "2",
            "spark.sql.session.timeZone": "UTC",
            "spark.sql.warehouse.dir": warehouse,
            "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
            "spark.sql.catalog.hive": "org.apache.iceberg.spark.SparkCatalog",
            "spark.sql.catalog.hive.type": "hive",
            "spark.sql.catalog.hive.uri": uri,
            "spark.sql.catalog.hive.warehouse": warehouse,
            "spark.jars": ",".join(str(self.root / "jars" / name) for name in
                                   ("iceberg.jar", "hadoop-aws.jar", "aws-sdk.jar")),
        }
        builder = SparkSession.builder.master("local[2]").appName("native-synthetic")
        for key, value in settings.items():
            builder.config(key, value)
        for key, value in hadoop.items():
            builder.config("spark.hadoop." + key, value)
        spark = builder.getOrCreate()
        try:
            require(spark.sparkContext.master == "local[2]", "Unexpected Spark master")
            conf = spark.sparkContext.getConf()
            require(all((spark.conf.get(key) if key.startswith("spark.sql.") else conf.get(key)) == value
                        for key, value in settings.items()), "Unexpected effective Spark configuration")
            effective = spark.sparkContext._jsc.hadoopConfiguration()
            require(all(effective.get(key) == value for key, value in hadoop.items()),
                    "Unexpected effective Hadoop configuration")
            return spark
        except BaseException:
            spark.stop()
            raise

    @contextmanager
    def session(self, unavailable_catalog=False):
        spark = self.new_spark(unavailable_catalog)
        try:
            yield spark
        finally:
            spark.stop()

    def initialize(self):
        from minio import Minio
        from scripts import bronze_to_silver_etl, rawData_to_bronze, silver_to_gold_etl

        client = Minio(MINIO_ENDPOINT, access_key=self.user, secret_key=self.password, secure=False)
        require(not client.bucket_exists(self.bucket), "Test bucket already exists. Use a fresh project")
        with self.session() as spark:
            namespaces = {row[0] for row in spark.sql("SHOW NAMESPACES IN hive").collect()}
            require(namespaces == {"default"}, "Metastore is not fresh")
            details = spark.sql("DESCRIBE NAMESPACE EXTENDED hive.default").collect()
            locations = [row[1] for row in details if row[0].lower() == "location"]
            require(locations == ["file:/native-meta/warehouse"] or
                    locations == ["file:///native-meta/warehouse"], "Unexpected HMS warehouse")
        client.make_bucket(self.bucket)
        self.objects = ScopedObjects(client, self.bucket)
        self.bronze = rawData_to_bronze
        self.silver = bronze_to_silver_etl
        self.gold = silver_to_gold_etl
        self.bronze.Minio = lambda *args, **kwargs: self.objects
        for module, layers in ((self.silver, ("bronze", "silver")), (self.gold, ("silver", "gold"))):
            module.Config.BUCKET = self.bucket
            module.Config.WAREHOUSE_PATH = f"{self.storage}/warehouse"
            for layer in layers:
                setattr(module.Config, f"{layer.upper()}_PATH", f"{self.storage}/{layer}")
            module.get_spark_session = lambda *args, **kwargs: self.new_spark()
        self.silver.Config.CURRENT_YEAR = 2026

    def ingest(self, name, content):
        with tempfile.TemporaryDirectory(prefix="synthetic-") as directory:
            path = Path(directory) / f"{name}.csv"
            path.write_bytes(content)
            self.bronze.upsert_to_minio(str(path), self.bucket, f"bronze/{name}.csv")


def main():
    root = Path(__file__).resolve().parent
    manifest, user, password = preflight(root)
    from scenarios import SCENARIOS

    scenario = manifest["scenario"]
    require(scenario in SCENARIOS, "Unknown native scenario")
    print(f"Candidate {manifest['candidate']} | scenario {scenario}", flush=True)
    run = NativeRun(root, manifest, user, password)
    run.initialize()
    SCENARIOS[scenario](run)
    print(f"Scenario completed: {scenario}", flush=True)


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        print(f"Native run failed ({type(error).__name__}). Inspect fixture-only diagnostics.",
              file=sys.stderr)
        sys.exit(1)
