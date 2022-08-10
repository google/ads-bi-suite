data "google_project" "project" {
}

data "google_billing_account" "account" {
  billing_account = data.google_project.project.billing_account

  lifecycle {
    postcondition {
      condition     = self.open
      error_message = "The project related billing account(${data.google_project.project.billing_account}) is not open."
    }
  }
}
