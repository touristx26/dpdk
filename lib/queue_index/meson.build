# SPDX-License-Identifier: BSD-3-Clause
# Copyright(c) 2026 Queue Index authors

if is_windows
    build = false
    reason = 'not supported on Windows'
    subdir_done()
endif

sources = files('rte_queue_index.c')
headers = files('rte_queue_index.h')
deps += ['ring', 'kvargs']
