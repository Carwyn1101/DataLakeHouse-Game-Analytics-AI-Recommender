"""
Weight Configuration for Game Recommendation System
====================================================

This module defines the weight distribution for feature importance
in the game recommendation algorithm.

Weight Distribution Strategy:
------------------------------
Total: 100% (1.0)

1. Genres (40%) - Primary Feature
   - Most important factor for game similarity
   - Users typically look for games in the same genre
   - Examples: RPG, Action, Strategy, etc.

2. Developers (30%) - Secondary Feature
   - Developer style and quality matter significantly
   - Games from same developer often share mechanics/quality
   - Examples: Valve, CD Projekt Red, FromSoftware

3. Categories (20%) - Tertiary Feature
   - Game features and mechanics
   - Examples: Multiplayer, Single-player, Co-op, VR Support

4. Price Category (5%) - Light Signal
   - Budget consideration for recommendations
   - Examples: Free, Low (<100k), Medium (100k-500k), High (>500k)

5. Review Positive Ratio (3%) - Light Signal
   - Quality indicator from user reviews
   - Range: 0.0 to 1.0 (0% to 100% positive)

6. Publishers (2%) - Very Light Signal
   - Publisher brand can indicate quality/style
   - Less important than developer
   - Examples: EA, Ubisoft, Activision

Mathematical Implementation:
-----------------------------
For each feature group, we:
1. Apply TF-IDF vectorization (for text features)
2. L2 normalize the vectors
3. Multiply by sqrt(weight) to apply weight
4. Concatenate all weighted features
5. Apply final L2 normalization

This ensures that the final similarity score is a weighted combination
of all features, with the specified importance distribution.

Author: Nguyễn Hồng Phúc (Carwyn1101)
Date: 2025
"""

# ===== WEIGHT CONFIGURATION =====
# FEATURE_WEIGHTS = {
#     'genres': 0.40,                           # 40% - Tập trung mạnh
#     'developers': 0.30,                       # 30% - Rất quan trọng
#     'categories': 0.20,                       # 20% - Quan trọng
#     'price_category': 0.05,                   # 5%  - Signal nhẹ
#     'review_positive_ratio': 0.03,            # 3%  - Signal nhẹ
#     'publishers': 0.02,                       # 2%  - Signal rất nhẹ
# }
# FEATURE_WEIGHTS = {
#     'tags': 0.35,                    # 35% - Trung tâm mới (chi tiết hơn genres)
#     'genres': 0.20,                  # 20% - Vẫn quan trọng nhưng nhường vai trò cho tags
#     'developers': 0.20,              # 20% - Giữ nguyên tầm quan trọng
#     'categories': 0.12,              # 12% - Giảm nhẹ
#     'price_category': 0.05,          # 5%  - Giữ nguyên
#     'review_positive_ratio': 0.05,   # 5%  - Tăng nhẹ
#     'publishers': 0.03,              # 3%  - Tăng nhẹ
# }

FEATURE_WEIGHTS = {
    'tags_top7': 0.49,               # 49% — 7 tags đầu
    'tags_remaining': 0.21,          # 21% — tags còn lại
    'developers': 0.15,              # 15%
    'categories': 0.08,              # 8%
    'price_category': 0.03,          # 3%
    'review_positive_ratio': 0.03,   # 3%
    'publishers': 0.01,              # 2%
}

# Validate weights sum to 1.0
assert abs(sum(FEATURE_WEIGHTS.values()) - 1.0) < 0.001, \
    f"Weights must sum to 1.0, got {sum(FEATURE_WEIGHTS.values())}"


# ===== FEATURE DIMENSIONS =====
# Total target dimension: ~500 features
# Distributed proportionally to weights

# FEATURE_DIMENSIONS = {
#     'genres': 200,          # 40% of 500
#     'developers': 150,      # 30% of 500
#     'categories': 100,      # 20% of 500
#     'price_category': 25,   # 5% of 500
#     'publishers': 10,       # 2% of 500
#     'review_ratio': 1,      # 3% - single numerical feature
# }

# FEATURE_DIMENSIONS = {
#     'tags': 200,         # 35% — nhiều nhất, tags có ~20 values/game (ELDEN RING có 20 tags)
#     'genres': 100,       # 20% — giảm từ 200 xuống
#     'developers': 100,   # 20% — giảm từ 150 xuống
#     'categories': 60,    # 12% — giảm từ 100 xuống
#     'price_category': 25,# 5%  — giữ nguyên
#     'publishers': 10,    # 3%  — giữ nguyên
#     'review_ratio': 1,   # 5%  — single numerical, giữ nguyên
# }

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
    
    # Sort by weight descending
    sorted_weights = sorted(FEATURE_WEIGHTS.items(), 
                           key=lambda x: x[1], 
                           reverse=True)
    
    for feature, weight in sorted_weights:
        pct = weight * 100
        bar_length = int(pct / 2)  # Scale to 50 chars max
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
    # Print weight configuration when run directly
    print(get_weight_info())
    print("\nWeight configuration is valid!")
