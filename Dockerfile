FROM zooniverse/ruby:2.2.1

#ADD https://nodejs.org/dist/v4.2.2/node-v4.2.2-linux-x64.tar.gz /usr/local/
#RUN tar -xvzf /usr/local/node-v4.2.2-linux-x64.tar.gz -C /usr/local/ --strip-components=1

ADD . /src
WORKDIR /src

RUN bundle install
#RUN npm install
