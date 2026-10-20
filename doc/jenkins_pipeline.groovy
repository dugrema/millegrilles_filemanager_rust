pipeline {
    agent { label 'x86_64' }

    parameters {
        string(defaultValue: 'master', name: 'BRANCH')
        string(defaultValue: '2026.6', name: 'VERSION')
        string(defaultValue: 'jenkins-maple', name: 'CREDENTIALS_ID')
        string(defaultValue: 'ssh://git.maple.maceroc.com/git/millegrilles_filemanager_rust', name: 'GIT_URL')
        string(defaultValue: 'registry.millegrilles.com:5000/millegrilles/filemanager_rust', name: 'DOCKER_IMAGE')
    }

    environment {
        NOM_APP="millegrilles_filemanager"
        VBUILD="${VERSION}.${BUILD_NUMBER}"
        DOCKER_IMAGE="${params.DOCKER_IMAGE}"
    }

    stages {
        stage('Checkout') {
            steps {
                checkout scmGit(branches: [[name: params.BRANCH]], extensions: [], userRemoteConfigs: [[credentialsId: params.CREDENTIALS_ID, url: params.GIT_URL]])
            }
        }

        stage('Build & Package & Deploy') {
            steps {
                sh "make deploy"
            }
        }

        stage('Archive') {
            steps {
                archiveArtifacts artifacts: 'target/release/*.gz', followSymlinks: false
            }
        }
    }
}
