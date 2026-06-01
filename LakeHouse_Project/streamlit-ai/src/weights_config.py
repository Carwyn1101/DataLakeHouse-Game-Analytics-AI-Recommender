"""
Weight and dimension configuration for FAISS game recommendation vectors.

`auto_builder.py` đọc hai dict này để quyết định mỗi nhóm feature đóng góp bao
nhiêu vào vector cuối cùng. Trọng số được áp dụng bằng cách L2-normalize từng
nhóm feature rồi nhân với sqrt(weight), sau đó concatenate và normalize lần cuối.
Thứ tự feature phải khớp với logic rebuild query vector trong `faiss_manager.py`.
"""

FEATURE_WEIGHTS = {
    'tags_top7': 0.49,               # 49% — 7 tags đầu
    'tags_remaining': 0.21,          # 21% — tags còn lại
    'developers': 0.15,              # 15%
    'categories': 0.08,              # 8%
    'price_category': 0.03,          # 3%
    'review_positive_ratio': 0.03,   # 3%
    'publishers': 0.01,              # 1%
}

assert abs(sum(FEATURE_WEIGHTS.values()) - 1.0) < 0.001, \
    f"Weights must sum to 1.0, got {sum(FEATURE_WEIGHTS.values())}"


FEATURE_DIMENSIONS = {
    'tags_top7': 150,        # 49% — top 7 tags, vocabulary nhỏ hơn vì chỉ 7 tags/game
    'tags_remaining': 100,   # 21% — remaining tags
    'developers': 80,        # 15%
    'categories': 50,        # 8%
    'price_category': 15,    # 3%
    'publishers': 10,        # 2%
    'review_ratio': 1,       # 3% — numerical
}

def get_weights():
    """
    Get the current weight configuration
    
    Returns:
        dict: Feature weights
    """
    return FEATURE_WEIGHTS.copy()


def get_dimensions():
    """
    Get the feature dimension configuration
    
    Returns:
        dict: Feature dimensions
    """
    return FEATURE_DIMENSIONS.copy()


def get_weight_info():
    """
    Get detailed information about weight configuration
    
    Returns:
        str: Formatted weight information
    """
    info = []
    info.append("=" * 60)
    info.append("GAME RECOMMENDATION WEIGHT CONFIGURATION")
    info.append("=" * 60)
    info.append("")
    
    sorted_weights = sorted(FEATURE_WEIGHTS.items(), 
                           key=lambda x: x[1], 
                           reverse=True)
    
    for feature, weight in sorted_weights:
        pct = weight * 100
        bar_length = int(pct / 2)
        bar = "█" * bar_length
        
        dim = FEATURE_DIMENSIONS.get(feature, 'N/A')
        
        info.append(f"{feature.upper():20s} {pct:5.1f}%  {bar}")
        info.append(f"{'':20s} Dimension: {dim}")
        info.append("")
    
    info.append("=" * 60)
    info.append(f"Total Weight: {sum(FEATURE_WEIGHTS.values()):.2f}")
    info.append(f"Total Dimensions: {sum(d for d in FEATURE_DIMENSIONS.values() if isinstance(d, int))}")
    info.append("=" * 60)
    
    return "\n".join(info)


if __name__ == "__main__":
    print(get_weight_info())
    print("\nWeight configuration is valid!")
