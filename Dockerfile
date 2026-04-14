FROM astral/uv:0.11-python3.13-trixie

RUN apt-get update \
    && apt-get install -y --no-install-recommends sudo git \
    && rm -rf /var/lib/apt/lists/*

RUN useradd -m -s /bin/bash user \
    && echo "user ALL=(ALL) NOPASSWD:ALL" > /etc/sudoers.d/user \
    && chmod 0440 /etc/sudoers.d/user

WORKDIR /app
COPY . .

RUN uv sync --all-groups \
    && chown -R user:user /app

USER user
ENV PATH="/app/.venv/bin:$PATH"
CMD ["/bin/bash"]
