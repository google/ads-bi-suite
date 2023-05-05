// Copyright 2023 Google Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
/**
 * @fileoverview Tentacles deployment file.
 */
'use strict';
const {cloudfunctions: {convertEnvPathToAbsolute}} = require(
    '@google-cloud/nodejs-common');

convertEnvPathToAbsolute('OAUTH2_TOKEN_JSON', __dirname);
convertEnvPathToAbsolute('API_SERVICE_ACCOUNT', __dirname);

Object.assign(module.exports,
    require('@google-cloud/gmp-googleads-connector'),
    require('@google-cloud/data-tasks-coordinator'));
