#!/usr/bin/env python3
"""
Debug script for Meta Ad Library API authentication issues.

This script provides detailed debugging information when authentication fails.

Usage:
    python scripts/debug_meta_auth.py
    python scripts/debug_meta_auth.py --token YOUR_TOKEN
"""

import argparse
import os
import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from dotenv import load_dotenv
import requests


def debug_token(token: str) -> dict:
    """Debug an access token and return detailed information."""
    results = {
        "token_info": {},
        "api_test": {},
        "recommendations": []
    }

    # Token basic info
    results["token_info"]["length"] = len(token)
    results["token_info"]["preview"] = token[:15] + "..." + token[-5:] if len(token) > 25 else "***"

    # Check token format
    if len(token) < 50:
        results["recommendations"].append(
            "Token seems too short. Meta access tokens are typically 150+ characters."
        )

    if " " in token or "\n" in token:
        results["recommendations"].append(
            "Token contains whitespace. Make sure you copied it correctly without extra spaces or newlines."
        )

    if token.startswith('"') or token.endswith('"'):
        results["recommendations"].append(
            "Token has quote characters. Remove surrounding quotes from the token."
        )

    # Test 1: Debug token endpoint
    print("\n1. Testing token with debug_token endpoint...")
    try:
        debug_url = "https://graph.facebook.com/debug_token"
        params = {
            "input_token": token,
            "access_token": token  # Self-inspect
        }
        response = requests.get(debug_url, params=params, timeout=30)
        debug_data = response.json()

        if "data" in debug_data:
            data = debug_data["data"]
            results["token_info"]["is_valid"] = data.get("is_valid", False)
            results["token_info"]["app_id"] = data.get("app_id")
            results["token_info"]["type"] = data.get("type")
            results["token_info"]["expires_at"] = data.get("expires_at")
            results["token_info"]["scopes"] = data.get("scopes", [])

            if not data.get("is_valid"):
                results["recommendations"].append(
                    "Token is INVALID. Generate a new token at https://developers.facebook.com/tools/explorer/"
                )

            if data.get("expires_at") == 0:
                print("   Token type: Never expires (page token or system user token)")
            elif data.get("expires_at"):
                import datetime
                exp_time = datetime.datetime.fromtimestamp(data["expires_at"])
                now = datetime.datetime.now()
                if exp_time < now:
                    results["recommendations"].append(
                        f"Token EXPIRED on {exp_time}. Generate a new token."
                    )
                else:
                    print(f"   Token expires: {exp_time}")
                    remaining = exp_time - now
                    if remaining.days < 7:
                        results["recommendations"].append(
                            f"Token expires in {remaining.days} days. Consider getting a long-lived token."
                        )

            # Check for ads_read permission
            scopes = data.get("scopes", [])
            print(f"   Scopes: {scopes}")
            if "ads_read" not in scopes:
                results["recommendations"].append(
                    "Token is missing 'ads_read' permission. Re-generate token with this permission."
                )

        elif "error" in debug_data:
            error = debug_data["error"]
            print(f"   Debug token error: {error.get('message')}")
            results["token_info"]["debug_error"] = error

    except Exception as e:
        print(f"   Failed to debug token: {e}")
        results["token_info"]["debug_error"] = str(e)

    # Test 2: Ad Library API access
    print("\n2. Testing Ad Library API access...")
    try:
        api_url = "https://graph.facebook.com/v19.0/ads_archive"
        params = {
            "access_token": token,
            "ad_reached_countries": "PL",
            "ad_type": "POLITICAL_AND_ISSUE_ADS",
            "fields": "id",
            "limit": 1
        }
        response = requests.get(api_url, params=params, timeout=30)

        print(f"   Status code: {response.status_code}")

        try:
            data = response.json()
        except ValueError:
            print(f"   Non-JSON response: {response.text[:200]}")
            results["api_test"]["error"] = "Non-JSON response"
            results["recommendations"].append(
                "API returned non-JSON response. This may indicate a network issue or API outage."
            )
            return results

        if response.status_code == 200:
            ad_count = len(data.get("data", []))
            print(f"   SUCCESS! Found {ad_count} test ad(s)")
            results["api_test"]["success"] = True
            results["api_test"]["ads_found"] = ad_count
        else:
            print(f"   API Error: {data}")
            results["api_test"]["success"] = False
            results["api_test"]["response"] = data

            if "error" in data:
                error = data["error"]
                error_code = error.get("code")
                error_subcode = error.get("error_subcode")
                error_message = error.get("message")

                print(f"   Error code: {error_code}")
                print(f"   Error subcode: {error_subcode}")
                print(f"   Error message: {error_message}")

                # Specific recommendations
                if error_code == 190:
                    if error_subcode == 463:
                        results["recommendations"].append(
                            "Token has EXPIRED. Generate a new token at https://developers.facebook.com/tools/explorer/"
                        )
                    else:
                        results["recommendations"].append(
                            "Token is INVALID. Check that you copied it correctly."
                        )
                elif error_code == 4 or error_code == 17:
                    results["recommendations"].append(
                        "Rate limit hit. Wait 5-10 minutes and try again."
                    )
                elif error_code == 10:
                    results["recommendations"].append(
                        "App does not have permission to access Ad Library API. "
                        "Ensure your app is set up correctly with 'ads_read' permission."
                    )
                elif error_code == 100:
                    results["recommendations"].append(
                        f"Invalid request parameter. The API version or fields may be incorrect."
                    )
                elif error_code == 200:
                    results["recommendations"].append(
                        "Permission denied. Your app may need Marketing API access approval."
                    )
                elif error_code == 2500:
                    results["recommendations"].append(
                        "App is not live or doesn't have proper permissions. "
                        "Check app settings at https://developers.facebook.com/apps/"
                    )

    except requests.RequestException as e:
        print(f"   Network error: {e}")
        results["api_test"]["error"] = str(e)
        results["recommendations"].append(
            f"Network error occurred: {e}. Check your internet connection."
        )

    return results


def main():
    parser = argparse.ArgumentParser(description="Debug Meta Ad Library API authentication")
    parser.add_argument(
        "--token",
        default=None,
        help="Access token to test (default: from META_ACCESS_TOKEN env var)"
    )
    args = parser.parse_args()

    # Load .env
    load_dotenv()

    # Get token
    token = args.token or os.getenv("META_ACCESS_TOKEN")

    if not token:
        print("ERROR: No access token provided.")
        print()
        print("Either:")
        print("  1. Set META_ACCESS_TOKEN in .env file")
        print("  2. Run with --token YOUR_TOKEN")
        print()
        print("To get a token:")
        print("  1. Go to https://developers.facebook.com/tools/explorer/")
        print("  2. Select your app")
        print("  3. Add 'ads_read' permission")
        print("  4. Generate token")
        return 1

    print("=" * 60)
    print("Meta Ad Library API - Authentication Debugger")
    print("=" * 60)

    results = debug_token(token)

    # Print recommendations
    print("\n" + "=" * 60)
    print("RECOMMENDATIONS")
    print("=" * 60)

    if not results["recommendations"]:
        if results.get("api_test", {}).get("success"):
            print("All tests passed! Your token is working correctly.")
        else:
            print("No specific recommendations, but API test failed.")
            print("Check the error messages above for more details.")
    else:
        for i, rec in enumerate(results["recommendations"], 1):
            print(f"\n{i}. {rec}")

    # Quick checklist
    print("\n" + "=" * 60)
    print("QUICK TROUBLESHOOTING CHECKLIST")
    print("=" * 60)
    print("""
1. Token format:
   - No quotes around the token in .env file
   - No extra spaces or newlines
   - Token should be 150+ characters

2. App setup (https://developers.facebook.com/apps/):
   - App must be "Live" (not in Development mode for production use)
   - Marketing API product must be added
   - 'ads_read' permission must be granted

3. Token permissions:
   - When generating token in Graph API Explorer, select 'ads_read'
   - Use "User or Page" token type

4. If token expired:
   - Short-lived tokens expire in ~1 hour
   - Exchange for long-lived token (60 days):

   curl "https://graph.facebook.com/v19.0/oauth/access_token?
     grant_type=fb_exchange_token&
     client_id=YOUR_APP_ID&
     client_secret=YOUR_APP_SECRET&
     fb_exchange_token=YOUR_SHORT_TOKEN"

5. API Access:
   - Ad Library API is publicly accessible
   - No special app review needed for reading public political ads
""")

    return 0 if results.get("api_test", {}).get("success") else 1


if __name__ == "__main__":
    sys.exit(main())
