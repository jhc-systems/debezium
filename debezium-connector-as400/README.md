
## build only directory structure for docker image

mvn assembly:single

## build ony docker image from structure above

mvn k8s:build
