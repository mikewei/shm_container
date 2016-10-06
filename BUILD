package(default_visibility = ["//visibility:public"])

cc_library(
	name = "shm_container",
	includes = ["src"],
	copts = [
		"-g",
		"-O2",
		"-Wall",
	],
	nocopts = "-fPIC",
  linkopts = [
    "-lrt",
  ],
	linkstatic = 1,
	srcs = glob([
		"src/shmc/*.cc",
		"src/shmc/*.h",
	]),
	deps = [],
)

cc_test(
	name = "test",
	copts = [
		"-g",
		"-O2",
		"-Wall",
  	"-fno-strict-aliasing",
  	"-DUNIT_TEST",
	],
	nocopts = "-fPIC",
	linkstatic = 1,
	srcs = glob(["test/*_test.cc"]),
	deps = [
		":shm_container",
		"//gtestx",
	],
)
