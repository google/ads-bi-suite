// Copyright 2021 Google Inc.
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

/** @fileoverview A type of data to be shown and operated in a sheet. */


/**
 * The sheet stores the currency exchange rate.
 */
class FxRateSheet extends DataTableSheet {

  get sheetName() {
    return 'fx rate';
  }

  get columnConfiguration() {
    return [
      { name: 'fromcur' },
      { name: 'tocur' },
      { name: 'rate' },
      {
        name: COLUMN_NAME_FOR_DEFAULT_CONFIG, width: 200,
        format: COLUMN_STYLES.MONO_FONT,
      },
    ];
  }

  get datasetProprtyName() {
    return 'configDataset';
  }

  get tableName() {
    return 'fx_rate_raw';
  }

  get initialData() {
    const sources = [
      'HKD', 'TWD', 'SGD', 'MYR', 'CNY', 'AUD', 'JPY', 'INR', 'AED', 'KRW',
      'CAD', 'EUR', 'GBP', 'ARS', 'KRW', 'RUB', 'VND', 'USD', 'CHF', 'GEL',
      'MOP', 'PHP', 'PKR', 'TRY', 'NZD',
    ];
    const targets = ['USD', 'AUD', 'SGD'];
    let index = 2;

    const result = [];

    for (let i = 0; i < targets.length; i++) {
      const target = targets[i];
      for (let j = 0; j < sources.length; j++) {
        const source = sources[j];
        result.push([
          source,
          target,
          `=IF(A${index}=B${index}, 1, GOOGLEFINANCE("CURRENCY:"&A${index}&B${index}))`
        ]);
        index++;
      }
    }

    return result;
  }
};
