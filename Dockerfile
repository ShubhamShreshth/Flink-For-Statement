FROM flink:latest
RUN apt-get update && \
    apt-get install -y maven && \
    apt-get install -y libglib2.0-0 libnspr4 libatk1.0-0 libatk-bridge2.0-0 libcups2 libdrm2 libdbus-1-3 libxcb1 libxkbcommon0 libatspi2.0-0 libx11-6 libxcomposite1 libxdamage1 libxext6 libxfixes3 libxrandr2 libgbm1 libpango-1.0-0 libcairo2 libasound2 && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*
EXPOSE 6123 8081
CMD ["/docker-entrypoint.sh","help"]