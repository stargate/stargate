# Stargate V2 - REST API

## General

This sub-project is for Quarkus-based stand-alone REST API
for Stargate V2, extracted from monolithic Stargate V1 Coordinator.
It does not use OSGi or run on a Coordinator: in its current form
all database access is by calling gRPC API which runs on a Coordinator node.

REST API runs as a [Quarkus](https://quarkus.io/) service, different from Stargate V1 APIs
which was build on DropWizard framework.

## Packaging

(TO BE WRITTEN)

## Configuration

(TO BE WRITTEN)

## Configuration options: General Quarkus

(TO BE WRITTEN)

## Configuration options: Stargate-specific

(TO BE WRITTEN)
