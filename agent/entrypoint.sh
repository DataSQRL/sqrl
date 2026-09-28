#!/usr/bin/env bash
#
# Copyright © 2021 DataSQRL (contact@datasqrl.com)
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# Start Pi in interactive mode. Provider credentials stay in environment
# variables so they are not written into the image or Pi's auth store.

set -euo pipefail

has_option() {
  local option="$1"
  shift
  local argument
  for argument in "$@"; do
    case "$argument" in
      "$option"|"$option"=*) return 0 ;;
    esac
  done
  return 1
}

# PI_PROVIDER and PI_MODEL are optional explicit selections. Without either,
# select a provider from the first available API key and let Pi choose one of
# that provider's available models. Pi still resolves the actual key from its
# standard provider environment variables.
provider="${PI_PROVIDER:-}"
model="${PI_MODEL:-}"

if [[ -z "$provider" && -n "$model" ]]; then
  model_lowercase="$(printf '%s' "$model" | tr '[:upper:]' '[:lower:]')"
  case "$model_lowercase" in
    anthropic/*|*claude*) provider="anthropic" ;;
    openai/*|*gpt-*|o[0-9]*|*codex*) provider="openai" ;;
    google/*|*gemini*) provider="google" ;;
    mistral/*|*mistral*|*codestral*) provider="mistral" ;;
    xai/*|*grok*) provider="xai" ;;
    groq/*) provider="groq" ;;
    cerebras/*) provider="cerebras" ;;
    deepseek/*) provider="deepseek" ;;
    openrouter/*) provider="openrouter" ;;
  esac
fi

if [[ -z "$provider" ]]; then
  if [[ -n "${OPENAI_API_KEY:-}" ]]; then
    provider="openai"
  elif [[ -n "${ANTHROPIC_API_KEY:-}" ]]; then
    provider="anthropic"
  elif [[ -n "${GEMINI_API_KEY:-}" ]]; then
    provider="google"
  elif [[ -n "${MISTRAL_API_KEY:-}" ]]; then
    provider="mistral"
  elif [[ -n "${XAI_API_KEY:-}" ]]; then
    provider="xai"
  elif [[ -n "${GROQ_API_KEY:-}" ]]; then
    provider="groq"
  elif [[ -n "${CEREBRAS_API_KEY:-}" ]]; then
    provider="cerebras"
  elif [[ -n "${DEEPSEEK_API_KEY:-}" ]]; then
    provider="deepseek"
  elif [[ -n "${OPENROUTER_API_KEY:-}" ]]; then
    provider="openrouter"
  fi
fi

pi_args=()

# Explicit Pi CLI options take precedence independently over their respective
# environment-based selections.
if ! has_option --provider "$@" && [[ -n "$provider" ]]; then
  pi_args+=(--provider "$provider")
fi

if ! has_option --model "$@" && [[ -n "$model" ]]; then
  pi_args+=(--model "$model")
fi

exec pi ${pi_args[@]+"${pi_args[@]}"} "$@"
