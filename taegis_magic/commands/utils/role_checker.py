from taegis_magic.core.service import get_service

def has_role(role: str, region: str):
    """Check if the current user has the specified role."""

    service = get_service(environment=region)
    user_info = service.users.query.current_tdruser()

    role_to_check = role.lower()

    return any(r.role_name.lower() == role_to_check for r in user_info.role_assignments)

