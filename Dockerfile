# Ninelives: API + dashboard + workers in one image. The dashboard is prebuilt into the package.
FROM python:3.13-slim

ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PIP_NO_CACHE_DIR=1 PIP_DISABLE_PIP_VERSION_CHECK=1
WORKDIR /app

COPY pyproject.toml README.md LICENSE ./
COPY ninelives ./ninelives
RUN pip install ".[all]"

COPY examples ./examples

RUN useradd --create-home --uid 10001 ninelives && chown -R ninelives /app
USER ninelives

EXPOSE 8000
# Override APP to run your own module, e.g. APP=myproject.workflows:app
ENV APP=examples/shop.py:app
CMD ["sh", "-c", "exec ninelives serve \"$APP\" --host 0.0.0.0 --port 8000"]
