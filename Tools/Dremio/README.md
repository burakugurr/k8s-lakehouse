# Dremio

## Purpose

This directory contains earlier Dremio values and migration notes retained for reference.

## Files

- dremio.yaml
- dremio-overrides.yaml

## Notes

This stack has moved to Trino + Polaris for the local lakehouse query layer.

## Production hardening

- Remove legacy Dremio deployment from active runtime if not needed
- Keep historical values only for migration analysis
- Revalidate with a proper production storage class and security policy before reactivation
