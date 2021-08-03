# Apps Google Ads reporting Automation

## 1. Disclaimer

Copyright 2021 Google LLC.
This is not an official Google product. This solution, including any related
sample code or data, is made available on an “as is,” “as available,” and “with
all faults” basis, solely for illustrative purposes, and without warranty or
representation of any kind. This solution is experimental, unsupported and
provided solely for your convenience. Your use of it is subject to your
agreements with Google, as applicable, and may constitute a beta feature as
defined under those agreements.  To the extent that you make any data available
to Google in connection with your use of the solution, you represent and warrant
that you have all necessary and appropriate rights, consents and permissions to
permit Google to use and process that data. By using any portion of this
solution, you acknowledge, assume and accept all risks, known and unknown,
associated with its usage, including with respect to your deployment of any
portion of this solution in your systems, or usage in connection with your
business, if at all.

## 2. Deployment

We suggest using a new Cloud Project to install this solution. Following
installation process should be run by the  __project owner__.

### 2.1 Preparation

Follow preparation doc to create authentication credentials and Google Ads
developer token.

Product    | Resource     | Notes
---------- | --------------- | ---------------------------------------
Google Ads | MCC account ID  | The top MCC account contains all the Ads accounts for reporting.
Google Ads | Developer token | Check the access level. Should be [Standard access].
Google Ads | User account    | Grant access to Google Ads accounts through OAuth during the installation.
GCP        | Billed GCP project ID  | This GCP shouldn’t engage with any other Google Ads developer token.
GCP        | User account    | Login in GCP and install the solution, ideally the 'owner'.
GCP        | OAuth client ID and client secret | The OAuth application’s type should be ‘Desktop app’.
Other      | Google account  | __Needs to be submitted before installation to be grant access.__

[Standard access]:https://developers.google.com/adwords/api/docs/access-levels#access_levels

### 2.2. Check out source codes

1. Open the [Cloud Shell](https://cloud.google.com/shell/)
2. Fellow this [instructions](https://g3doc.corp.google.com/company/teams/gtech/ads/das/cse/faq/tools/professional-services-googlesource-com.md#working-from-gcp) to clone the repository:

    ```shell
    git clone https://professional-services.googlesource.com/solutions/ads-bi-suite
    ```

### 2.3. Run install script

Run the deployment scrip, and follow the prompt to enter your client id,
client secret, Google Ads developer token, and mcc id.

   ```shell
   cd ads-bi-suite; chmod a+x deploy.sh; ./deploy.sh
   ```

### 2.4. Possible extra tasks during installation

#### 2.4.1. Initialize Firestore

If the GCP hasn't got the Firestore (Datastore) initialized, during the
installation, the script will print a link and ask you to create Firestore in
the opened page before continue. The prompt looks like this:

```shell
Cannot find Firestore or Datastore in current project. Please visit
https://console.cloud.google.com/firestore?project=[YOUR_PROJECT_ID] to
create a database before continue.

Press any key to continue after you create the database...
```

## 3. Release

Follow [go/lego-release](go/lego-release) process, we release the system
bi-weekly. The detail features/bugs between each release version are documented
in [go/lego-release-note](go/lego-release-note).

### 3.1 Command

Command to fetch the codebase for a specific release version.

```shell
git clone https://professional-services.googlesource.com/solutions/ads-bi-suite && cd ads-bi-suite && git checkout $VERSION_TAG
```

_Due to [b/192634659](b/192634659), use git commit id (9b8e740) to fetch this version instead of version tag._

### 3.2 Versions

Follow preparation doc to create authentication credentials and Google Ads
developer token.

| Solution    | Demo Dashboard Link |
| ---------- | ------------ |
| Advertiser | [go/lego4advertiser-demo](go/lego4advertiser-demo) |
| Agency | [go/lego4agency-demo](go/lego4agency-demo) |


| Version    | Release Date | Worklog    | BigQuery Schema Change | Note |
| ---------- | ------------ | -----------| ----------------| ------------------------|
| v.20210705 | 2021-07-05   | [b/192903846](b/192903846) | Yes | Advertiser: Add Video Campaign asset report. Agency: Add auto-bidding UI. |
| v.20210719 | 2021-07-19   | [b/194749564](b/194749564) | Yes | Advertiser: ADH Pure; Key BP Metrics Tracking. Agency: Bid low alert, AC cannibalization alert. |
| v.20210802 | 2021-08-02   | [b/195254576](b/195254576) | Yes | Advertiser: ADH Pure Dashboard. Agency: Support multiple MCCs. (**Please be noted that this version requires the reinstallation.**) |
