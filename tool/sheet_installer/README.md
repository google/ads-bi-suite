LEGO has a Google Sheets based installation tool (Code name Cyborg). This tool replaces the previous Bash script `deploy.sh` with enhanced features:

1. Logs LEGO installation events and keeps a record of all affected GCP resources.
1. Upgrades to new versions or installs specific versions of the backend Google Cloud Function.
1. Enables new connectors post-LEGO installation.
1. Edits and uploads API configurations within the Google Sheet.
1. Generates and manages OAuth tokens, stored in GCP Secret Manager.
1. Supports different credentials (OAuth tokens) for various integration configurations.
1. Tests API configuration accessibility directly within the Sheets.
1. Tests installed LEGO by sending data from the Sheet to the target Cloud Storage bucket.

**Key Differences:**

1. Cannot deploy customized LEGO without additional adaptation. See the developer section for details.
1. Cannot deploy OAuth token files or service account key files locally:
    * OAuth token files: Store them in Secret Manager or use the tool to create new tokens.
    * Service account key files: Deprecated due to security concerns. Use the Cloud Functions service account (displayed in the Sheet).


## For Deployment:

## 1. Preparation

**1.1. Create/use a Google Cloud project with a billing account:**

1. [How to Create and Manage Projects](https://cloud.google.com/resource-manager/docs/creating-managing-projects#creating_a_project)
1. [How to Create, Modify, or Close Your Billing Account](https://cloud.google.com/billing/docs/how-to/manage-billing-account#create_a_new_billing_account)

**1.2. Create/check the OAuth consent screen:**

If there is no OAuth consent screen in this Google Cloud project, you need to [configure the OAuth consent screen](https://developers.google.com/workspace/guides/configure-oauth-consent) first. When you create the consent screen, some settings need to be:

1. Publishing status: **In production** (otherwise, refresh tokens expire every 7 days).
1. User type: **External** (or see [this link](https://support.google.com/cloud/answer/10311615?hl=en#zippy=%2Cexternal%2Cinternal) for details).
1. Scopes: no need to be entered.


**1.3. Generate the Google Sheets based installation tool:**

1. **Enable the Google Apps Script API**
1. **Get [clasp](https://github.com/google/clasp) ready:**
    1. Install Node.js and npm if not present.
    1. In a local terminal:
        1. `npm install -g @google/clasp`
        1. `clasp login` (Default credentials saved to: `~/.clasprc.json`)
1. **Create your customized LEGO on Cyborg:**
    1. Navigate to `tool/sheet_installer/src/lego/apps_script` folder.
    1. Initialize: `npm exec cyborg init --solution=lego --target=install`
    1. Deploy: `npm exec cyborg deploy --solution=lego --target=install`
    1. **Update Google Cloud project number** for the Apps Script (see 1.4).


**1.4. Update Google Cloud project number for the Apps Script:**

1. Get your GCP project number. See [how to determine the project number of a standard Cloud project](https://developers.google.com/apps-script/guides/cloud-platform-projects#determine_the_id_number_of_a_standard)
1. Open Apps Script editor (Extensions > Apps Script).
1. On the Apps Script window, click ⚙️ (Project Settings) at the left menu bar, then click the button Change project.
1. Enter the project number and click Set project.

**NOTE:** If you encounter an error about switching projects outside your organization, set up [explicit authorization](https://github.com/GoogleCloudPlatform/cloud-for-marketing/blob/main/marketing-analytics/activation/gmp-googleads-connector/tutorials/install_tentacles_in_google_sheets.md#16-optional-create-an-explicit-authorzation).

**1.5. (Optional) Create an OAuth 2.0 client ID:**

Some APIs require OAuth. See [Create an OAuth 2.0 client ID](https://developers.google.com/workspace/guides/create-credentials#oauth-client-id) (select "Desktop" application type).

**1.6. (Optional) Create an Explicit Authorization:**

Prefer using 1.4. This is a workaround:

1. Create an OAuth Client (see 1.5). Use a separate client for data integration.
2. Click `Cyborg` > `Explicit Authorization`.
3. Enter the OAuth client ID and secret.
4. Click `Start` and complete the OAuth process (expect a "This site can't be reached" error page).
5. Copy the error page URL and paste it into the sidebar.
6. Cyborg will generate a refresh token.
7. Click `Save as explicit authorization`.


## 2. Understand the tool:

**2.1. Sheets and Menu (`Cyborg`):**
* **README:** Information sheet
* Step 1 - Setting LEGO Configurations
* Secret Manager
* Step 2 - Generate an OAuth Token - for LEGO Installation
* Step 3 - Validate API Access with OAuth Token
* Step 4 - Upload SQL Files
* Step 5 - Upload Task Config Files
* Step 6 - Set Up Daily/Hourly/ADH Cronjobs
* Explicit authorization

**For Developer:**

1. Configure OAuth consent screen (See instructions above.)

2. **Configure OAuth consent screen:** (As described above in 1.2)

3. **Prepare your environment:**
    1.  Google Cloud project (with billing enabled).
    1.  OAuth consent screen configured.
    1.  Desktop OAuth client created.

4. **Get [clasp](https://github.com/google/clasp) ready:**
    1. Enable the Google Apps Script API.
    1. Install Node.js and npm if necessary.
    1. In your terminal:
        1.  `npm install -g @google/clasp`
        1. `clasp login`

5. **Develop your customized LEGO:**
    1. Work in the `tool/sheet_installer/src/lego/apps_script` directory.
    1. Initialize: `npm exec cyborg init --solution=lego --target=customized`
    1. Debug (local): `npm exec cyborg debug --solution=lego --target=customized`
    1. Deploy: `npm exec cyborg deploy --solution=lego --target=customized`
    1. Update the Google Cloud **project number** within the generated spreadsheet's Apps Script.
    1. Test your customized LEGO directly in the generated spreadsheet.

(Steps for developers are also included, but follow a similar pattern to the initial setup.)


## 3. Installation

How to install LEGO: (1) switch to sheet `Step 1 - Setting LEGO Configurations` and input
required information in the sheet; (2) use menu `Check resources` to run
a check. If an error happened, fix it and retry `Check resources`; (3) after
all checks passed, use menu `Apply changes` to complete the installation. (4) Uploads needed setting via multiple sheets and menus.

### 3.1. Input required information in the Sheet `Step 1 - Setting LEGO Configurations` and apply the changes

This sheet contains a list of Cloud resources that will be operated during
installation. You do not need to edit most of them except:

1. Yellow background fields that need user input or confirm, e.g. `Project Id`.
1. Tick checkboxes to select `Connectors` that you are going to use.

### 3.2. Menu `🤖 Cyborg` -> `Step1 - Setting LEGO Configurations`

When you click items under `🤖 Cyborg` for the first time, a dialog window
titled `Authorization Required` will pop up to ask for confirmation.
After you complete it, just click the menu item again as you originally clicked.

#### 3.2.1. Submenu item `Check resources`

This item does the most jobs with no or minor changes to the GCP project. If
anything wrong happened, it would pause and mark the related resource's `status`
as `ERROR` and the reason would be appended.

Based on the GCP's situation, it might pause several times especially when a
new GCP project is involved, including:

1. Ask users to select the `Location` for Cloud Functions and Cloud Storage
1. Ask users to select the mode and location to create a `Firestore` instance

You can always re-run `Check resources` after you fix the problems or after you
make any changes, e.g. select another version of LEGO. All passed resources
have the `status` as `OK`


#### 3.2.2. `Step 2 - Prepare OAuth Token`

##### 3.2.2.1.1. Fetch OAuth Token from lagecy LEGO cloud function

1. Click menu `🤖 Cyborg` -> `Secret Manager`. This will open a
   sidebar titled `Save OAuth token in deployed Cloud Functions to Secret Manager`.

##### 3.2.2.1.2. (Optional) Generate an OAuth token via OAuth sidebar

1. Click menu `🤖 Cyborg` -> `Step 2 - Generate an OAuth Token - for LEGO Installation`. This will open a
   sidebar titled `OAuth2 token generator`.
1. Enter the `OAuth client ID` and `client secret` that you created previously.
1. Select the API scopes that you want to grant access.
1. Click the `Start` button and complete the OAuth confirmation process in the
   newly opened tab and land on an error page _"This site can't be reached"_.
   **This is an expected behaviour.**
1. Copy the `url` of the error page and paste it back to the `OAuth` sidebar.
1. `Cyborg` would complete the OAuth process and put a refresh token in the
   `textarea` named `Generated OAuth token`.
1. Enter a name for this token to be saved in `Secret Manager`.
1. Click the button `Save` and wait for it to complete.

Now you can use the saved token through the secret name in API configuration.

#### 3.2.2.2. Sheet `Secret Manager`

After you saved the OAuth token in the sidebar, you can list it in the sheet
`Secret Manager` by click menu `🤖 Cyborg` -> `Secret Manager`
-> `Refresh secrets`.

#### 3.2.3. Input `Secret Manager` Name to `Step1 - Setting LEGO Configurations` sheet and `Check resources` via Menu `🤖 Cyborg` -> `Step1 - Setting LEGO Configurations` -> `Check resources`.

After you prepare the OAuth token, switch back to `Step1 - Setting LEGO Configurations` sheet and input your needed `Secret Manager` Name from sheet `Secret Manger` to `Step1 - Setting LEGO Configurations`.

And then click Munu `🤖 Cyborg` -> `Step1 - Setting LEGO Configurations` -> `Check  resources`.


#### 3.2.4. Submenu item `🤖 Cyborg` -> `Step1 - Setting LEGO Configurations` -> `Apply changes`

After all resources have passed the check, some resources have the `status` as
`TO_APPLY`. These resources are usually major changes and need
users to confirm before the process, e.g. deploying Cloud Functions, creating a
new Cloud Storage bucket, creating a new BigQueery dataset, etc.

Click menu `Apply changes` to apply those changes.

> **NOTE:** For non Google Workspace accounts, the script runtime is up to 6 min
> / execution. So possibly an `exceeded max execution time` occurs when you are
> deploying all three Cloud Functions. If that happens, just click `Apply changes`
> again.

#### 3.2.5. Submenu item `Recheck resources (even it is OK)`

For efficiency, `Cyborg` skips most of all `OK` resources when it carries out
the task `Check resources`. However in some circumstances, if you would like to
have a forced `Check` everything, you can use this item.


### 3.3. Sheet `Step 3 - Validate API Access with OAuth Token`

This sheet contains a list of Cloud resources that will be operated during
installation. You do not need to edit most of them except:

1. Yellow background fields that need user input or confirm, e.g. `Project Id` in `Step 1 - Setting LEGO Configurations`.
1. Click menu `🤖 Cyborg` -> `Step3 - Validate API Access with OAuth Token` -> `Refresh secret name list`. This will pull the secret managers to sheet `Step3 - Validate API Access with OAuth Token`.
1. Input your `Secret Manager` name to `Step3 - Validate API Access with OAuth Token` sheet.
   1. Secret Name: Select your used Secret Manager Name. For upgrade, the secret manager should be `lego_main_legacy_token`.
   1. API: Select your used API, `Google Ads Report`.
   1. Resource Id: Your Google Ads MCC CID.
   1. Extra Info: Your Google Ads developer token.
   1. Click menu `🤖 Cyborg` -> `Step3 - Validate API Access with OAuth Token` -> `Check All APIs` to test your OAuth Token is valid for the API or not.

### 3.4. Sheet `Step 4. Upload SQL Files`

1. Click menu `🤖 Cyborg` -> `Step4 - Upload SQL Files` -> `Check All APIs` -> `Uploads all files`.
2. (Optional) If you customized LEGO SQL files before and you want to keep what you did, you need to upload the sql files again manually.

### 3.5. Sheet `Step5 - Upload Task Config Files`

1. (Optional) If you customized LEGO task config before and you want to keep what you did, you need to click `🤖 Cyborg` -> `Step5 - Upload Task Config Files` -> `Append all configs from Firestore to Sheet`.
1. Click menu `🤖 Cyborg` -> `Step5 - Upload Task Config Files` -> `Check All APIs` -> `Uploads all files`.
1. Click menu `🤖 Cyborg` -> `Step5 - Upload Task Config Files` -> `Check All APIs` -> `Uploads all files`.

### 3.5. Sheet `Step6 - Set Up Daily/Hourly/ADH Cronjobs`

1. **(Action Required for ADH Creative installation)**: Check the checkbox in `Enabled` column to enable `#namespace#-adh_lego_start` for `LEGO ADH Creative job`.
1. **(Action Required for ADH Audience installation)**: Check the checkbox in `Enabled` column to enable  `#namespace#-adh_audience_start` for `LEGO ADH Audience job`.
1. Click menu `🤖 Cyborg` -> `Step6 - Set Up Daily/Hourly/ADH Cronjobs` -> `Uploads all jobs to Cloud Scheduler`.
2. Entry Google Cloud console to enable and trigger `#namespace#-lego_start` job.
