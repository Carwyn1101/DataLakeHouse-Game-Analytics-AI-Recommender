# Bronze synthetic fixture. The MinIO release asset was checked against its signed upstream release.
FROM registry.access.redhat.com/ubi9/ubi-micro@sha256:c55b3d2c48728e4a4ebc3f63a716e5ba135a5f44097667eaea8ccd3f8404fa66
ADD --checksum=sha256:eef6581f6509f43ece007a6f2eb4c5e3ce41498c8956e919a7ac7b4b170fa431 --chmod=0755 https://github.com/minio/minio/releases/download/RELEASE.2025-07-23T15-54-02Z/minio.linux-amd64.RELEASE.2025-07-23T15-54-02Z /usr/bin/minio
USER 0
EXPOSE 9000
VOLUME ["/data"]
ENTRYPOINT ["/usr/bin/minio"]
CMD ["server", "/data", "--address", ":9000"]
