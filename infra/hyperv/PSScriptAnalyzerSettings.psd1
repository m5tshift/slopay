@{
    Severity     = @('Error', 'Warning')
    # Pester variables defined in BeforeAll and read in It blocks look unused to the analyzer.
    ExcludeRules = @('PSUseDeclaredVarsMoreThanAssignments')
}
