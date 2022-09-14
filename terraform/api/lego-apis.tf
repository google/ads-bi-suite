resource "google_project_service" "lego_apis" {
  for_each = toset(flatten(concat([for k, v in var.lego_functions : lookup(var.apis_map, k) if v ])))

  service = "${each.key}"
  disable_dependent_services = true
}
