package golmdb

import _ "embed"

// LMDB is licensed under The OpenLDAP Public License version
// 2.8. That has the requirement that:
//
//   Redistributions must contain a verbatim copy of this document
//
// Hence this variable embeds the license itself. As a matter of
// simplicity, this Go binding of LMDB, golmdb, is also licensed under
// the same The OpenLDAP Public License version 2.8.
//
//go:embed LICENSE
var License string
