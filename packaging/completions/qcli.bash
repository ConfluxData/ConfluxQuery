_qcli() {
  local current previous
  current="${COMP_WORDS[COMP_CWORD]}"
  previous="${COMP_WORDS[COMP_CWORD-1]}"
  case "$previous" in
    --config|--target|--file|--bind|--auth-file|--cors-origin) return ;;
    --format) COMPREPLY=($(compgen -W "table vertical csv tsv json jsonl" -- "$current")); return ;;
  esac
  COMPREPLY=($(compgen -W "--help --version --config --target --command --file --format config target auth serve" -- "$current"))
}
complete -F _qcli qcli
