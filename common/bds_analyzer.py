import base64
import re
from typing import Dict

BDS_PERCENT_THRESHOLD = 6.0
HIGH_THREAT_PERCENT_THRESHOLD = 14.0

high_raw = base64.b64decode("R2Vub2NpZGUsV2FyIENyaW1lcyxBcGFydGhlaWQsTWFzc2FjcmUsTmFrYmEsRGlzcGxhY2VtZW50LEh1bWFuaXRhcmlhbiBDcmlzaXMsQmxvY2thZGUsT2NjdXBhdGlvbixSZWZ1Z2VlcyxJQ0MsQkRT").decode("utf-8").split(",")
medium_raw = base64.b64decode("RnJlZWRvbSBGbG90aWxsYSxSZXNpc3RhbmNlLExpYmVyYXRpb24sRnJlZSBQYWxlc3RpbmUsR2F6YSxDZWFzZWZpcmUsUHJvdGVzdCxVTlJXQQ==").decode("utf-8").split(",")

INDICATORS_DICT = {}
for word in high_raw:
    INDICATORS_DICT[word.lower()] = 2
for word in medium_raw:
    INDICATORS_DICT[word.lower()] = 1


def clean_text(text: str) -> str:
    text = text.lower()
    return re.sub(r"[^a-z0-9]+", " ", text).strip()

def analyze_transcription(transcription: str) -> Dict[str, object]:
    text = clean_text(transcription)
    total_words = max(len(text.split()), 1) 
    
    padded_text = f" {text} " 
    
    weighted_hits = 0
    matches = []

    for term, weight in INDICATORS_DICT.items():
        padded_term = f" {term} "
        count = padded_text.count(padded_term)
        
        if count > 0:
            weighted_hits += count * weight
            matches.append({"term": term, "count": count, "weight": weight})

    bds_percent = (weighted_hits / total_words) * 100
    bds_percent = round(bds_percent, 2)

    if bds_percent >= HIGH_THREAT_PERCENT_THRESHOLD:
        threat_level = "high"
    elif bds_percent >= BDS_PERCENT_THRESHOLD:
        threat_level = "medium"
    else:
        threat_level = "none"

    return {
        "bds_percent": bds_percent,
        "is_bds": bds_percent >= BDS_PERCENT_THRESHOLD,
        "bds_threat_level": threat_level,
        "bds_matches": matches,
    }