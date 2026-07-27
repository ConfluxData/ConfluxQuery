Register-ArgumentCompleter -Native -CommandName qcli -ScriptBlock {
    param($wordToComplete, $commandAst, $cursorPosition)
    @(
        '--help', '--version', '--config', '--target', '--command', '--file',
        '--format', 'config', 'target', 'auth', 'serve'
    ) | Where-Object { $_ -like "$wordToComplete*" } | ForEach-Object {
        [System.Management.Automation.CompletionResult]::new($_, $_, 'ParameterValue', $_)
    }
}
