# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
"""
handler.py

AWS Lambda handler for the DSQL-backed pet store API, invoked through API
Gateway (REST, Lambda proxy integration). The API is intentionally
unauthenticated for end users -- it is a public sample API -- but it is
reachable only through CloudFront.

Two layers enforce "CloudFront only":
  1. The API Gateway resource policy denies any request whose secret header
     does not match (primary enforcement, in the backend template).
  2. This handler re-verifies the same secret header as defense in depth, using
     a constant-time comparison, and fails closed if the expected secret is not
     configured. The secret is injected by CloudFront as a custom origin header
     and supplied to the Lambda via the CF_SECRET_HEADER_VALUE env var. It is
     never baked into source or templates.

Routes:
  POST   /pets          create a pet
  GET    /pets          list pets
  GET    /pets/{petId}  fetch one pet
"""

import hmac
import json
import logging
import os

import db

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Name of the header CloudFront injects. Value is a secret provided at deploy
# time via environment; absent/empty means fail closed (deny everything).
SECRET_HEADER_NAME = os.environ.get("CF_SECRET_HEADER_NAME", "x-origin-verify")
EXPECTED_SECRET = os.environ.get("CF_SECRET_HEADER_VALUE", "")


def _response(status: int, body) -> dict:
    return {
        "statusCode": status,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps(body),
    }


def _headers_lower(event) -> dict:
    return {k.lower(): v for k, v in (event.get("headers") or {}).items()}


def _cloudfront_verified(event) -> bool:
    """Constant-time check of the CloudFront secret header. Fail closed."""
    if not EXPECTED_SECRET:
        # The verifier is not configured -> deny, never allow.
        logger.error("CF_SECRET_HEADER_VALUE is not set; denying all requests (fail closed).")
        return False
    presented = _headers_lower(event).get(SECRET_HEADER_NAME.lower(), "")
    return hmac.compare_digest(presented, EXPECTED_SECRET)


def handler(event, context):
    # Enforce CloudFront-only access before touching the data layer.
    if not _cloudfront_verified(event):
        return _response(403, {"message": "Forbidden"})

    method = event.get("httpMethod", "")
    resource = event.get("resource", "")
    path_params = event.get("pathParameters") or {}

    try:
        if resource == "/pets" and method == "POST":
            return _create_pet(event)
        if resource == "/pets" and method == "GET":
            return _list_pets(event)
        if resource == "/pets/{petId}" and method == "GET":
            return _get_pet(path_params.get("petId"))
        if resource == "/pets/{petId}" and method == "PUT":
            return _update_pet(event, path_params.get("petId"))
        if resource == "/pets/{petId}" and method == "DELETE":
            return _delete_pet(path_params.get("petId"))
        return _response(404, {"message": "Not found"})
    except ValueError as e:
        return _response(400, {"message": str(e)})
    except Exception:  # noqa: BLE001 - surface a clean 500, log the detail
        logger.exception("Unhandled error processing request")
        return _response(500, {"message": "Internal server error"})


def _create_pet(event) -> dict:
    try:
        payload = json.loads(event.get("body") or "{}")
    except json.JSONDecodeError:
        raise ValueError("Request body must be valid JSON")

    pet_type = (payload.get("petType") or "").strip()
    pet_name = (payload.get("petName") or "").strip()
    if not pet_type:
        raise ValueError("petType is required")
    if not pet_name:
        raise ValueError("petName is required")

    pet_age = payload.get("petAge", 0)
    try:
        pet_age = int(pet_age)
    except (TypeError, ValueError):
        raise ValueError("petAge must be an integer")

    pet = db.create_pet(pet_type, pet_name, pet_age)
    return _response(201, pet)


def _list_pets(event) -> dict:
    qs = event.get("queryStringParameters") or {}
    try:
        limit = int(qs.get("limit", 50))
    except (TypeError, ValueError):
        limit = 50
    return _response(200, {"pets": db.list_pets(limit)})


def _get_pet(pet_id: str) -> dict:
    if not pet_id:
        raise ValueError("petId is required")
    pet = db.get_pet(pet_id)
    if pet is None:
        return _response(404, {"message": "Pet not found"})
    return _response(200, pet)


def _update_pet(event, pet_id: str) -> dict:
    if not pet_id:
        raise ValueError("petId is required")
    try:
        payload = json.loads(event.get("body") or "{}")
    except json.JSONDecodeError:
        raise ValueError("Request body must be valid JSON")

    pet_type = payload.get("petType")
    pet_name = payload.get("petName")
    pet_age = payload.get("petAge")
    if pet_type is None and pet_name is None and pet_age is None:
        raise ValueError("provide at least one of petType, petName, petAge")
    if pet_age is not None:
        try:
            pet_age = int(pet_age)
        except (TypeError, ValueError):
            raise ValueError("petAge must be an integer")

    pet = db.update_pet(pet_id, pet_type=pet_type, pet_name=pet_name, pet_age=pet_age)
    if pet is None:
        return _response(404, {"message": "Pet not found"})
    return _response(200, pet)


def _delete_pet(pet_id: str) -> dict:
    if not pet_id:
        raise ValueError("petId is required")
    deleted = db.delete_pet(pet_id)
    if not deleted:
        return _response(404, {"message": "Pet not found"})
    # 204 No Content: empty body.
    return {"statusCode": 204, "headers": {}, "body": ""}
