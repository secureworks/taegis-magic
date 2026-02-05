from taegis_magic.core.service import get_service

def has_role(role: str, region: str) -> bool:
    """Check if the current subject has the specified role."""
    
    with get_service(exclude_deprecated_output=False, environment=region) as service:
        subject_info = service.subjects.query.current_subject()

    role_to_check = role.lower()
    return any(r.role_name.lower() == role_to_check for r in subject_info.role_assignment_data.role_assignments)
