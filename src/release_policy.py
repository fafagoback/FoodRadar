"""One store-failure budget shared by capture, Raw replay and recovery."""
MAX_FAILURE_PERCENT = 5


def failure_budget_allows(failed: int, total: int) -> bool:
    if type(total) is not int or type(failed) is not int or total <= 0 or not 0 <= failed <= total:
        raise ValueError("Invalid store failure counts")
    return failed * 100 <= total * MAX_FAILURE_PERCENT


def require_failure_budget(failed: int, total: int):
    if not failure_budget_allows(failed, total):
        raise ValueError(f"Total store failure rate exceeds 5%: {failed}/{total} ({100*failed/total:.4f}%)")


def validate_partial_manifest(meta, *, enforce_budget=True):
    stores = meta.get("stores")
    if not isinstance(stores, list):
        raise ValueError("Missing per-store Raw records for partial replay")
    passed = [s for s in stores if s.get("success") is True]
    failed = [s for s in stores if s.get("success") is not True]
    ids = [s.get("store_uuid") for s in stores]
    if (not passed or not failed or not all(ids) or len(set(ids)) != len(ids)
            or meta.get("feed_pages", 0) < 1
            or meta.get("successful_store_count") != len(passed)
            or meta.get("failed_store_count") != len(failed)
            or meta.get("failed_store_uuids") != [s.get("store_uuid") for s in failed]):
        raise ValueError("Partially verified Raw manifest store count mismatch")
    if enforce_budget:
        require_failure_budget(len(failed), len(stores))
    return passed, failed
