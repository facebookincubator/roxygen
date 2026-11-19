/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

use http::Extensions;

pub(crate) struct RetiringExtensions(pub(crate) Extensions);

impl RetiringExtensions {
    pub(crate) fn into_inner(mut self) -> Extensions {
        std::mem::take(&mut self.0)
    }
}

impl Drop for RetiringExtensions {
    fn drop(&mut self) {
        // Keep the table owned during entry destruction so unwinding also
        // deallocates it when an extension's destructor panics.
        self.0.clear();
    }
}
