variable "workspace_name" {
  description = "Unique display name for the ephemeral Fabric workspace."
  type        = string
}

variable "capacity_id" {
  description = "ID of the existing paid Fabric capacity used by integration tests."
  type        = string
}

variable "workspace_description" {
  description = "Description used by operators and stale-workspace cleanup."
  type        = string
  default     = "Ephemeral fabricQueryR integration-test workspace"
}

variable "test_principal_id" {
  description = "Optional Entra object ID of a principal other than the workspace creator."
  type        = string
  default     = null
  nullable    = true
}

variable "test_principal_type" {
  description = "Principal type used for test_principal_id."
  type        = string
  default     = "ServicePrincipal"

  validation {
    condition = contains(
      ["Group", "ServicePrincipal", "ServicePrincipalProfile", "User"],
      var.test_principal_type
    )
    error_message = "test_principal_type must be Group, ServicePrincipal, ServicePrincipalProfile, or User."
  }
}

variable "test_principal_role" {
  description = "Workspace role granted to test_principal_id when it is set."
  type        = string
  default     = "Contributor"

  validation {
    condition = contains(
      ["Admin", "Contributor", "Member", "Viewer"],
      var.test_principal_role
    )
    error_message = "test_principal_role must be Admin, Contributor, Member, or Viewer."
  }
}
