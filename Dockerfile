FROM public.ecr.aws/lambda/nodejs:16

LABEL maintainer="PRX <sysadmin@prx.org>"
LABEL org.prx.spire.publish.s3="LAMBDA_ZIP"

WORKDIR /app

ENTRYPOINT [ "yarn", "run" ]
CMD [ "test" ]

RUN yum install -yq zip rsync && yum clean all && rm -rf /var/cache/yum
ADD yarn.lock ./
ADD package.json ./
RUN npm install --quiet --global yarn && yarn install
ADD . .
RUN yarn run build
