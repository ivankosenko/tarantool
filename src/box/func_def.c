#include "func_def.h"
#include "opt_def.h"

const char *func_language_strs[] = {"LUA", "C"};

const struct func_opts func_opts_default = {
	/* .is_deterministic = */ false,
};

const struct opt_def func_opts_reg[] = {
	OPT_DEF("is_deterministic", OPT_BOOL, struct func_opts, is_deterministic),
};
