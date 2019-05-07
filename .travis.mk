#
# Travis CI rules
#

DOCKER_IMAGE?=packpack/packpack:debian-stretch

all: package

package:
	git clone https://github.com/packpack/packpack.git packpack
	./packpack/packpack

test: test_$(TRAVIS_OS_NAME)

# Redirect some targets via docker
test_linux: docker_test_ubuntu
coverage: docker_coverage_ubuntu

docker_%:
	mkdir -p ~/.cache/ccache
	docker run \
		--rm=true --tty=true \
		--volume "${PWD}:/tarantool" \
		--volume "${HOME}/.cache:/cache" \
		--workdir /tarantool \
		-e XDG_CACHE_HOME=/cache \
		-e CCACHE_DIR=/cache/ccache \
		-e COVERALLS_TOKEN=${COVERALLS_TOKEN} \
		-e TRAVIS_JOB_ID=${TRAVIS_JOB_ID} \
		-e CMAKE_EXTRA_PARAMS=${CMAKE_EXTRA_PARAMS} \
		-e CC=${CC} \
		-e CXX=${CXX} \
		${DOCKER_IMAGE} \
		make -f .travis.mk $(subst docker_,,$@)

deps_ubuntu:
	apt-get -y update && apt-get install -y -f \
		build-essential cmake coreutils sed \
		libreadline-dev libncurses5-dev libyaml-dev libssl-dev \
		libcurl4-openssl-dev libunwind-dev libicu-dev \
		python python-pip python-setuptools python-dev \
		python-msgpack python-yaml python-argparse python-six python-gevent \
		lcov ruby clang llvm llvm-dev

deps_buster_clang_8:
	echo "deb http://apt.llvm.org/buster/ llvm-toolchain-buster-8 main" > /etc/apt/sources.list.d/clang_8.list
	echo "deb-src http://apt.llvm.org/buster/ llvm-toolchain-buster-8 main" >> /etc/apt/sources.list.d/clang_8.list
	wget -O - https://apt.llvm.org/llvm-snapshot.gpg.key | sudo apt-key add -
	apt-get -y update
	apt-get -y install clang-8 llvm-8 llvm-8-dev

build_ubuntu:
	cmake . -DCMAKE_BUILD_TYPE=RelWithDebInfo -DENABLE_WERROR=ON ${CMAKE_EXTRA_PARAMS}
	make -j

runtest_ubuntu: build_ubuntu
	cd test && /usr/bin/python test-run.py --force -j 50 --no-output-timeout -1

test_ubuntu: deps_ubuntu runtest_ubuntu

deps_osx:
	brew update
	brew install openssl readline curl icu4c --force
	curl --silent --show-error --retry 5 https://bootstrap.pypa.io/get-pip.py | python
	pip install -r test-run/requirements.txt

test_osx: deps_osx
	cmake . -DCMAKE_BUILD_TYPE=RelWithDebInfo -DENABLE_WERROR=ON ${CMAKE_EXTRA_PARAMS}
	# Increase the maximum number of open file descriptors on macOS
	sudo sysctl -w kern.maxfiles=20480 || :
	sudo sysctl -w kern.maxfilesperproc=20480 || :
	sudo launchctl limit maxfiles 20480 || :
	ulimit -S -n 20480 || :
	ulimit -n
	make -j
	cd test && python test-run.py --force -j 50 unit/ app/ app-tap/ box/ box-tap/

build_cov_ubuntu:
	cmake . -DCMAKE_BUILD_TYPE=Debug -DENABLE_GCOV=ON
	find -name "*.gcda" -exec rm -rf {} \;
	make -j

runtest_cov_ubuntu: build_cov_ubuntu
	# Enable --long tests for coverage
	cd test && /usr/bin/python test-run.py --force -j 50 --long --no-output-timeout -1
	lcov --compat-libtool --directory src/ --capture --output-file coverage.info.tmp
	lcov --compat-libtool --remove coverage.info.tmp 'tests/*' 'third_party/*' '/usr/*' \
		--output-file coverage.info
	lcov --list coverage.info
	@if [ -n "$(COVERALLS_TOKEN)" ]; then \
		echo "Exporting code coverage information to coveralls.io"; \
		gem install coveralls-lcov; \
		echo coveralls-lcov --service-name travis-ci --service-job-id $(TRAVIS_JOB_ID) --repo-token [FILTERED] coverage.info; \
		coveralls-lcov --service-name travis-ci --service-job-id $(TRAVIS_JOB_ID) --repo-token $(COVERALLS_TOKEN) coverage.info; \
	fi;

coverage_ubuntu: deps_ubuntu runtest_cov_ubuntu

source:
	git clone https://github.com/packpack/packpack.git packpack
	TARBALL_COMPRESSOR=gz packpack/packpack tarball

# Push alpha and beta versions to <major>x bucket (say, 2x),
# stable to <major>.<minor> bucket (say, 2.2).
ifeq ($(TRAVIS_BRANCH),master)
GIT_DESCRIBE=$(shell git describe HEAD)
MAJOR_VERSION=$(word 1,$(subst ., ,$(GIT_DESCRIBE)))
MINOR_VERSION=$(word 2,$(subst ., ,$(GIT_DESCRIBE)))
else
MAJOR_VERSION=$(word 1,$(subst ., ,$(TRAVIS_BRANCH)))
MINOR_VERSION=$(word 2,$(subst ., ,$(TRAVIS_BRANCH)))
endif
BUCKET=tarantool.$(MAJOR_VERSION).$(MINOR_VERSION).src
ifeq ($(MINOR_VERSION),0)
BUCKET=tarantool.$(MAJOR_VERSION)x.src
endif
ifeq ($(MINOR_VERSION),1)
BUCKET=tarantool.$(MAJOR_VERSION)x.src
endif

source_deploy:
	pip install awscli --user
	aws --endpoint-url "${AWS_S3_ENDPOINT_URL}" s3 \
		cp build/*.tar.gz "s3://${BUCKET}/" \
		--acl public-read
