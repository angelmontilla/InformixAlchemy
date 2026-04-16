# SPDX-License-Identifier: Apache-2.0

from sqlalchemy.testing.provision import temp_table_keyword_args


@temp_table_keyword_args.for_db("informix")
def _informix_temp_table_keyword_args(cfg, eng):
    return {"prefixes": ["TEMP"]}
