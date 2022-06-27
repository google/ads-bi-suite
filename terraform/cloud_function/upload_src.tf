locals {
    source_files = [
        "../index.js",
        "../package.json",
    ]
    source_key = [
        "../keys/oauth2.token.json",
    ]
}

data "template_file" "t_file" {
  count = "${length(local.source_files)}"

  template = "${file(element(local.source_files, count.index))}"
}

data "template_file" "t_key" {
  count = "${length(local.source_key)}"

  template = "${file(element(local.source_key, count.index))}"
}

resource "local_file" "files_to_temp_dir" {
    count    = "${length(local.source_files)}"
    filename = "./.tmp/src/${basename(element(local.source_files, count.index))}"
    content  = "${element(data.template_file.t_file.*.rendered, count.index)}"
}

resource "local_file" "key_to_temp_dir" {
    count    = "${length(local.source_key)}"
    filename = "./.tmp/src/keys/${basename(element(local.source_key, count.index))}"
    content  = "${element(data.template_file.t_key.*.rendered, count.index)}"
}

data "archive_file" "cloud_function_source" {
    type        = "zip"

    source_dir  = "./.tmp/src"
    output_path = "./.tmp/function.zip"

    depends_on   = [
       local_file.files_to_temp_dir,
       local_file.key_to_temp_dir
    ]
}

# Add source code zip to the Cloud Function's bucket
resource "google_storage_bucket_object" "cloud_function_zip" {
    source       = data.archive_file.cloud_function_source.output_path
    content_type = "application/zip"

    # Append to the MD5 checksum of the files's content
    # to force the zip to be updated as soon as a change occurs
    name         = "src-${data.archive_file.cloud_function_source.output_md5}.zip"
    bucket       = "${var.namespace}-${var.project_id}-function"

    # Dependencies are automatically inferred so these lines can be deleted
    depends_on   = [
        data.archive_file.cloud_function_source
    ]
}