# Dependencies are vendored for linux/arm64 by `make api-vendor` (uv resolves for the target platform), so the
# image build needs no network access to PyPI. Pinned versions live in api/requirements.txt.
FROM python:3.12-slim
ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=/srv:/opt/vendor
WORKDIR /srv
COPY api/vendor /opt/vendor
COPY api /srv/api
COPY common /srv/common
RUN rm -rf /srv/api/vendor && useradd -r -u 10001 apiuser
USER apiuser
EXPOSE 8000
CMD ["python", "-m", "uvicorn", "api.main:app", "--host", "0.0.0.0", "--port", "8000", "--no-access-log"]
