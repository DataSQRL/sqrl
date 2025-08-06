#!/usr/bin/env python3
"""
JWT Token Generation Utility for DataSQRL JWT Authorization Tests

This utility generates JWT tokens for testing JWT-based authorization in DataSQRL.
The tokens are compatible with the jwt-authorized test suite configuration.

REQUIREMENTS:
    pip install PyJWT

USAGE:
    1. Edit the PAYLOAD variable below to set the desired JWT claims
    2. Run: python3 generate_jwt_tokens.py
    3. Copy the generated token to your test .properties file

JWT CONFIGURATION:
    The tokens are generated using the secret key and parameters from package.json:
    - Secret: testSecretThatIsAtLeast256BitsLong32Chars (base64: dGVzdFNlY3JldFRoYXRJc0F0TGVhc3QyNTZCaXRzTG9uZzMyQ2hhcnM=)
    - Algorithm: HS256
    - Issuer: my-test-issuer  
    - Audience: ["my-test-audience"]
    - Default Expiration: 9999999999 (far future)

SQRL FUNCTIONS:
    - AuthMyTable(val BIGINT): Expects JWT claim "val" with single integer value
    - AuthMyTableValues(val ARRAY<BIGINT>): Expects JWT claim "values" with array of integers

EXAMPLES:
    # For AuthMyTable with val=73, edit PAYLOAD to:
    # PAYLOAD = {
    #     "iss": "my-test-issuer",
    #     "aud": ["my-test-audience"],
    #     "exp": 9999999999,
    #     "val": 73
    # }
    
    # For AuthMyTableValues with values=[1,2,82], edit PAYLOAD to:
    # PAYLOAD = {
    #     "iss": "my-test-issuer", 
    #     "aud": ["my-test-audience"],
    #     "exp": 9999999999,
    #     "values": [1, 2, 82]
    # }
"""

import jwt
import json

# JWT configuration matching package.json
SECRET = "testSecretThatIsAtLeast256BitsLong32Chars"
ALGORITHM = "HS256"

# ==============================================================================
# EDIT THIS PAYLOAD TO GENERATE DIFFERENT TOKENS
# ==============================================================================
# Change the payload below and run this script to generate a new JWT token
PAYLOAD = {
    "iss": "my-test-issuer",
    "aud": ["my-test-audience"],
    "exp": 9999999999,
    "values": [1, 2, 82]  # Example: for AuthMyTableValues with values 1, 2, 82
    # "val": 73           # Example: for AuthMyTable with single value 73
    # "val": None         # Example: for null value
    # (omit val/values)   # Example: for no-val scenarios
}
# ==============================================================================


def generate_token():
    """Generate JWT token using the hardcoded PAYLOAD."""
    token = jwt.encode(PAYLOAD, SECRET, algorithm=ALGORITHM)
    return token


def decode_token(token):
    """Decode and verify JWT token."""
    try:
        decoded = jwt.decode(token, SECRET, algorithms=[ALGORITHM], 
                           audience=PAYLOAD["aud"], issuer=PAYLOAD["iss"])
        return decoded
    except jwt.InvalidTokenError as e:
        return f"Invalid token: {e}"


def main():
    """Generate token using the hardcoded PAYLOAD and display results."""
    print("=" * 80)
    print("JWT Token Generator for DataSQRL JWT Authorization Tests")
    print("=" * 80)
    
    print("\nCurrent Payload:")
    print(json.dumps(PAYLOAD, indent=2))
    
    token = generate_token()
    
    print(f"\nGenerated JWT Token:")
    print(token)
    
    print(f"\nUse in .properties file:")
    print(f"Authorization: Bearer {token}")
    
    # Verify by decoding
    print(f"\nVerification (decoded payload):")
    decoded = decode_token(token)
    if isinstance(decoded, dict):
        print(json.dumps(decoded, indent=2))
    else:
        print(f"Error: {decoded}")
    
    print("\n" + "=" * 80)
    print("To generate different tokens:")
    print("1. Edit the PAYLOAD variable in this script")
    print("2. Run this script again")
    print("=" * 80)


if __name__ == "__main__":
    main()