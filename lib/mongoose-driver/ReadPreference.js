// Copyright 2015 rain1017.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.

/* jshint ignore:start */

/*!
 * Module dependencies.
 */

var mongodb = require('mongodb');
var ReadPref = mongodb.ReadPreference;

/*!
 * Converts arguments to ReadPrefs the driver
 * can understand.
 *
 * @param {String|Array} pref
 * @param {Array} [tags]
 */

module.exports = function readPref (pref, tags) {
  if (Array.isArray(pref)) {
    tags = pref[1];
    pref = pref[0];
  }

  if (pref instanceof ReadPref) {
    return pref;
  }

  switch (pref) {
    case 'p':
      pref = 'primary';
      break;
    case 'pp':
      pref = 'primaryPreferred';
      break;
    case 's':
      pref = 'secondary';
      break;
    case 'sp':
      pref = 'secondaryPreferred';
      break;
    case 'n':
      pref = 'nearest';
      break;
  }

  return new ReadPref(pref, tags);
}
