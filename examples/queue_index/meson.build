# SPDX-License-Identifier: BSD-3-Clause
# Copyright(c) 2026 Queue Index authors

if not is_linux
    build = false
    reason = 'only supported on Linux'
    subdir_done()
endif

allow_experimental_apis = true
deps = ['eal', 'ring', 'queue_index']
sources = files('main.c')
