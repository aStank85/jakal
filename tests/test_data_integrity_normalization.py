from src.database import Database


def test_queue_key_normalizes_unknown_to_other():
    assert Database._canonicalize_queue_key("Ranked") == "ranked"
    assert Database._canonicalize_queue_key("Unranked") == "standard"
    assert Database._canonicalize_queue_key("Quick Match") == "quickmatch"
    assert Database._canonicalize_queue_key("Arcade Event") == "event"
    assert Database._canonicalize_queue_key("superrankedmode") == "other"
    assert Database._canonicalize_queue_key("ultra_unrankedish") == "other"
    assert Database._canonicalize_queue_key("mystery_queue") == "other"


def test_operator_name_normalization_preserves_known_and_uses_unknown():
    assert Database._canonicalize_operator_name("Jäger") == "Jager"
    assert Database._canonicalize_operator_name(" Tubarão ") == "Tubarao"
    assert Database._canonicalize_operator_name("unknown") == "UNKNOWN"
    assert Database._canonicalize_operator_name("totally_new_operator") == "UNKNOWN"
    registry_outputs = set(Database.OPERATOR_DISPLAY_BY_KEY.values()) | {"UNKNOWN"}
    for sample in ["Jäger", "jaeger", "Tubarão", "Ace", "unknown", "totally_new_operator"]:
        assert Database._canonicalize_operator_name(sample) in registry_outputs
