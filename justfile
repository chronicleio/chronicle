release:
    cargo build --release
    mkdir -p distribution
    cp target/release/lyra distribution/

fetch-tla-tools:
    mkdir -p tla+/.tools
    cd tla+/.tools && \
        wget https://github.com/tlaplus/tlaplus/releases/download/v1.8.0/tla2tools.jar && \
        wget https://github.com/tlaplus/CommunityModules/releases/download/202211012231/CommunityModules-deps.jar

tla:
    cd tla+ && \
        java -XX:+UseParallelGC -DTLA-Library=.tools/CommunityModules-deps.jar -jar .tools/tla2tools.jar -workers auto Lyra.tla