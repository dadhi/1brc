#!/bin/bash

dotnet build -c Release
dotnet publish -r win-x64 -c Release