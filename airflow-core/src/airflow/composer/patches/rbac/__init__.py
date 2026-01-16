"""
Composer RBAC patch.

Changes:
- patch FastAPI OAuth2PasswordBearer to use Inverting Proxy user ID header as an auth token
- implement Composer Auth Manager
- implement Composer Airflow Security Manager
- implement Composer Auth Remote User View
- implement Per-Folder Roles Autoregistration feature
"""
