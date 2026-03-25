#!/usr/bin/env python3
"""
Test script for migration 009 - Hero Stats Materialized Views

Verifies:
1. Materialized views exist and are populated
2. RPC functions work and return data
3. Refresh function works
4. Performance is acceptable

Run after applying migration 009:
    python scripts/test_migration_009.py
"""

import sys
import time
from pathlib import Path

# Add parent directory to path so we can import from db.py and secrets_loader.py
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from secrets_loader import load_secrets

load_secrets()

from db import get_creator_hero_stats, init_supabase, refresh_hero_stats_cache


def test_views_exist():
    """Test that materialized views were created."""
    print("\n1️⃣  Testing materialized views exist...")

    client = init_supabase()
    if not client:
        print("❌ FAIL: Supabase client not available")
        return False

    try:
        # Test mv_hero_stats
        resp = client.rpc("get_creator_hero_stats").execute()
        if not resp.data:
            print("❌ FAIL: mv_hero_stats is empty")
            return False
        print("✅ PASS: mv_hero_stats exists and has data")

        # Test mv_lists_meta
        resp = client.rpc("get_lists_meta").execute()
        if not resp.data:
            print("❌ FAIL: mv_lists_meta is empty")
            return False
        print("✅ PASS: mv_lists_meta exists and has data")

        return True

    except Exception as e:
        print(f"❌ FAIL: Error checking views: {e}")
        return False


def test_rpc_performance():
    """Test RPC function performance."""
    print("\n2️⃣  Testing RPC performance...")

    try:
        # Test get_creator_hero_stats
        start = time.time()
        stats = get_creator_hero_stats()
        duration_ms = (time.time() - start) * 1000

        if not stats:
            print("❌ FAIL: get_creator_hero_stats returned empty dict")
            return False

        print(f"✅ PASS: get_creator_hero_stats returned in {duration_ms:.1f}ms")
        print(f"   Data: {stats}")

        if duration_ms > 100:
            print(
                f"⚠️  WARNING: Slow response time ({duration_ms:.1f}ms). "
                "Expected <100ms for materialized view."
            )

        return True

    except Exception as e:
        print(f"❌ FAIL: Error testing RPC: {e}")
        return False


def test_refresh_function():
    """Test refresh function."""
    print("\n3️⃣  Testing refresh function...")

    try:
        start = time.time()
        result = refresh_hero_stats_cache()
        duration_ms = (time.time() - start) * 1000

        if not result["success"]:
            print(f"❌ FAIL: Refresh failed: {result['error']}")
            return False

        print(f"✅ PASS: Refresh completed in {duration_ms:.1f}ms")

        for view in result["materialized_views"]:
            print(f"   • {view['name']}: {view['rows']} rows, {view['duration_ms']}ms")

        return True

    except Exception as e:
        print(f"❌ FAIL: Error testing refresh: {e}")
        return False


def test_data_integrity():
    """Test that returned data makes sense."""
    print("\n4️⃣  Testing data integrity...")

    try:
        stats = get_creator_hero_stats()

        # Check required keys exist
        required_keys = [
            "total_creators",
            "avg_engagement",
            "has_engagement_data",
            "growing_creators",
            "premium_creators",
        ]

        for key in required_keys:
            if key not in stats:
                print(f"❌ FAIL: Missing key '{key}' in stats")
                return False

        # Check data types
        if not isinstance(stats["total_creators"], int):
            print(f"❌ FAIL: total_creators should be int, got {type(stats['total_creators'])}")
            return False

        if not isinstance(stats["avg_engagement"], (int, float)):
            print(f"❌ FAIL: avg_engagement should be numeric, got {type(stats['avg_engagement'])}")
            return False

        if not isinstance(stats["has_engagement_data"], bool):
            print(
                f"❌ FAIL: has_engagement_data should be bool, got {type(stats['has_engagement_data'])}"
            )
            return False

        for key in ("growing_creators", "premium_creators"):
            value = stats[key]
            if not isinstance(value, int):
                print(f"❌ FAIL: {key} should be int, got {type(value)}")
                return False
            if value < 0:
                print(f"❌ FAIL: {key} should be non-negative, got {value}")
                return False

        # Check for unexpected keys (schema drift detection)
        unexpected_keys = set(stats.keys()) - set(required_keys)
        if unexpected_keys:
            print(f"⚠️  WARNING: Unexpected keys in stats: {sorted(unexpected_keys)}")
            # Don't fail - just warn about schema drift

        # Check ranges
        if stats["total_creators"] < 0:
            print(f"❌ FAIL: total_creators is negative: {stats['total_creators']}")
            return False

        if not 0 <= stats["avg_engagement"] <= 100:
            print(f"⚠️  WARNING: avg_engagement outside expected range: {stats['avg_engagement']}")

        print("✅ PASS: Data integrity checks passed")
        print(f"   Total creators: {stats['total_creators']:,}")
        print(f"   Avg engagement: {stats['avg_engagement']:.2f}%")
        print(f"   Growing creators: {stats['growing_creators']:,}")
        print(f"   Premium creators: {stats['premium_creators']:,}")

        return True

    except Exception as e:
        print(f"❌ FAIL: Error checking data integrity: {e}")
        return False


def main():
    """Run all tests."""
    print("=" * 70)
    print("Testing Migration 009: Hero Stats Materialized Views")
    print("=" * 70)

    init_supabase()

    tests = [
        ("Views Exist", test_views_exist),
        ("RPC Performance", test_rpc_performance),
        ("Refresh Function", test_refresh_function),
        ("Data Integrity", test_data_integrity),
    ]

    results = []
    for name, test_func in tests:
        try:
            passed = test_func()
            results.append((name, passed))
        except Exception as e:
            print(f"\n❌ EXCEPTION in {name}: {e}")
            results.append((name, False))

    # Summary
    print("\n" + "=" * 70)
    print("Test Summary")
    print("=" * 70)

    passed = sum(1 for _, result in results if result)
    total = len(results)

    for name, result in results:
        status = "✅ PASS" if result else "❌ FAIL"
        print(f"{status}: {name}")

    print(f"\n{passed}/{total} tests passed")

    if passed == total:
        print("\n🎉 All tests passed! Migration 009 is working correctly.")
        return 0
    else:
        print(f"\n⚠️  {total - passed} test(s) failed. Check logs above.")
        return 1


if __name__ == "__main__":
    sys.exit(main())
