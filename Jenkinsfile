#!groovy

node {
    def TIDB_BRANCH = "rc2.2"
    def TIKV_BRANCH = "rc2.2"
    def PD_BRANCH = "rc2.2"

    fileLoader.withGit('git@github.com:pingcap/SRE.git', 'master', 'github-iamxy-ssh', '') {
        fileLoader.load('jenkins/ci/pingcap_tidb_binlog_branch.groovy').call(TIDB_BRANCH, TIKV_BRANCH, PD_BRANCH)
    }
}
