$Log_MaskableKeys = @(
    'password'
)


#
# System functions
#

function Idm-SystemInfo {
    param (
        # Operations
        [switch] $Connection,
        [switch] $TestConnection,
        [switch] $Configuration,
        # Parameters
        [string] $ConnectionParams
    )

    Log info "-Connection=$Connection -TestConnection=$TestConnection -Configuration=$Configuration -ConnectionParams='$ConnectionParams'"
    
    if ($Connection) {
        @(
            @{
                name = 'host_name'
                type = 'textbox'
                label = 'Server'
                description = 'IP or Hostname of Server'
                value = ''
            }
            @{
                name = 'port'
                type = 'textbox'
                label = 'Port'
                description = 'Instance port'
                value = '22501'
            }
            @{
                name = 'database'
                type = 'textbox'
                label = 'Database'
                description = 'Name of database'
                value = 'SKYWARD'
            }
             @{
                name = 'driver_version'
                type = 'textbox'
                label = 'Driver Version'
                description = 'Version of Progress OpenEdge Driver'
                value = '11.7'
            }
            @{
                name = 'enableVPN'
                type = 'checkbox'
                label = 'Use VPN'
                value = $true
            }
            @{
                name = 'vpnOpenPath'
                type = 'textbox'
                label = 'Open VPN Path'
                description = 'Path to script start connection to vpn'
                value = 'C:\\Tools4ever\\Scripts\\connectSkywardVPN.bat'
            }
            @{
                name = 'vpnClosePath'
                type = 'textbox'
                label = 'Close VPN Path'
                description = 'Path to script close connection to vpn'
                value = 'C:\\Tools4ever\\Scripts\\disconnectSkywardVPN.bat'
            }
            @{
                name = 'user'
                type = 'textbox'
                label = 'Username'
                label_indent = $true
                description = 'User account name to access server'
            }
            @{
                name = 'password'
                type = 'textbox'
                password = $true
                label = 'Password'
                label_indent = $true
                description = 'User account password to access server'
            }
            @{
                name = 'isolation_mode'
                type = 'textbox'
                label = 'Isolation Mode'
                value = 'READ UNCOMMITED'
            }
            @{
                name = 'array_size'
                type = 'textbox'
                label = 'Array Size'
                value = '50'
            }
            @{
                name = 'enableETWT'
                type = 'checkbox'
                label = 'Enable ETWT'
                value = $true
            }
            @{
                name = 'enableUWCT'
                type = 'checkbox'
                label = 'Enable UWCT'
                value = $true
            }
            @{
                name = 'enableKA'
                type = 'checkbox'
                label = 'Enable KA'
                value = $true
            }
            @{
                name = 'table_1_name'
                type = 'textbox'
                label = 'Query 1 - Name of Table'
                description = ''
            }
            @{
                name = 'table_1_query'
                type = 'textbox'
                label = 'Query 1 - SQL Statement'
                description = ''
            }
            @{
                name = 'table_2_name'
                type = 'textbox'
                label = 'Query 2 - Name of Table'
                description = ''
            }
            @{
                name = 'table_2_query'
                type = 'textbox'
                label = 'Query 2 - SQL Statement'
                description = ''
            }
            @{
                name = 'table_3_name'
                type = 'textbox'
                label = 'Query 3 - Name of Table'
                description = ''
            }
            @{
                name = 'table_3_query'
                type = 'textbox'
                label = 'Query 3 - SQL Statement'
                description = ''
            }
            @{
                name = 'table_4_name'
                type = 'textbox'
                label = 'Query 4 - Name of Table'
                description = ''
            }
            @{
                name = 'table_4_query'
                type = 'textbox'
                label = 'Query 4 - SQL Statement'
                description = ''
            }
            @{
                name = 'table_5_name'
                type = 'textbox'
                label = 'Query 5 - Name of Table'
                description = ''
            }
            @{
                name = 'table_5_query'
                type = 'textbox'
                label = 'Query 5 - SQL Statement'
                description = ''
            }
            @{
                name = 'table_6_name'
                type = 'textbox'
                label = 'Query 6 - Name of Table'
                description = ''
            }
            @{
                name = 'table_6_query'
                type = 'textbox'
                label = 'Query 6 - SQL Statement'
                description = ''
            }
            @{
                name = 'table_7_name'
                type = 'textbox'
                label = 'Query 7 - Name of Table'
                description = ''
            }
            @{
                name = 'table_7_query'
                type = 'textbox'
                label = 'Query 7 - SQL Statement'
                description = ''
            }
            @{
                name = 'table_8_name'
                type = 'textbox'
                label = 'Query 8 - Name of Table'
                description = ''
            }
            @{
                name = 'table_8_query'
                type = 'textbox'
                label = 'Query 8 - SQL Statement'
                description = ''
            }
            @{
                name = 'table_9_name'
                type = 'textbox'
                label = 'Query 9 - Name of Table'
                description = ''
            }
            @{
                name = 'table_9_query'
                type = 'textbox'
                label = 'Query 9 - SQL Statement'
                description = ''
            }
            @{
                name = 'table_10_name'
                type = 'textbox'
                label = 'Query 10 - Name of Table'
                description = ''
            }
            @{
                name = 'table_10_query'
                type = 'textbox'
                label = 'Query 10 - SQL Statement'
                description = ''
            }
            @{
                name = 'table_11_name'
                type = 'textbox'
                label = 'Query 11 - Name of Table'
                description = ''
            }
            @{
                name = 'table_11_query'
                type = 'textbox'
                label = 'Query 11 - SQL Statement'
                description = ''
            }
            @{
                name = 'table_12_name'
                type = 'textbox'
                label = 'Query 12 - Name of Table'
                description = ''
            }
            @{
                name = 'table_12_query'
                type = 'textbox'
                label = 'Query 12 - SQL Statement'
                description = ''
            }
            @{
                name = 'table_13_name'
                type = 'textbox'
                label = 'Query 13 - Name of Table'
                description = ''
            }
            @{
                name = 'table_13_query'
                type = 'textbox'
                label = 'Query 13 - SQL Statement'
                description = ''
            }
            @{
                name = 'table_14_name'
                type = 'textbox'
                label = 'Query 14 - Name of Table'
                description = ''
            }
            @{
                name = 'table_14_query'
                type = 'textbox'
                label = 'Query 14 - SQL Statement'
                description = ''
            }
            @{
                name = 'table_15_name'
                type = 'textbox'
                label = 'Query 15 - Name of Table'
                description = ''
            }
            @{
                name = 'table_15_query'
                type = 'textbox'
                label = 'Query 15 - SQL Statement'
                description = ''
            }
            @{
                name = 'table_16_name'
                type = 'textbox'
                label = 'Query 16 - Name of Table'
                description = ''
            }
            @{
                name = 'table_16_query'
                type = 'textbox'
                label = 'Query 16 - SQL Statement'
                description = ''
            }
            @{
                name = 'table_17_name'
                type = 'textbox'
                label = 'Query 17 - Name of Table'
                description = ''
            }
            @{
                name = 'table_17_query'
                type = 'textbox'
                label = 'Query 17 - SQL Statement'
                description = ''
            }
            @{
                name = 'table_18_name'
                type = 'textbox'
                label = 'Query 18 - Name of Table'
                description = ''
            }
            @{
                name = 'table_18_query'
                type = 'textbox'
                label = 'Query 18 - SQL Statement'
                description = ''
            }
            @{
                name = 'table_19_name'
                type = 'textbox'
                label = 'Query 19 - Name of Table'
                description = ''
            }
            @{
                name = 'table_19_query'
                type = 'textbox'
                label = 'Query 19 - SQL Statement'
                description = ''
            }
            @{
                name = 'table_20_name'
                type = 'textbox'
                label = 'Query 20 - Name of Table'
                description = ''
            }
            @{
                name = 'table_20_query'
                type = 'textbox'
                label = 'Query 20 - SQL Statement'
                description = ''
            }
        )
    }

    if ($TestConnection) {
        Open-ProgressDBConnection $ConnectionParams

        
        $query = "SELECT TBL 'Name', 'Table' `"Type`"  FROM sysprogress.SYSTABLES_FULL WHERE TBLTYPE = 'T' ORDER BY TBL"
        $sql_command = New-ProgressDBCommand $Query
        $result = (Invoke-ProgressDBCommand $sql_command)
    }

    if ($Configuration) {
        @()
    }

    Log info "Done"
}


function Idm-OnUnload {
    Close-ProgressDBConnection
}


#
# CRUD functions
#

$ColumnsInfoCache = @{}


function Idm-Dispatcher {
    param (
        # Optional Class/Operation
        [string] $Class,
        [string] $Operation,
        # Mode
        [switch] $GetMeta,
        # Parameters
        [string] $SystemParams,
        [string] $FunctionParams
    )

    Log info "-Class='$Class' -Operation='$Operation' -GetMeta=$GetMeta -SystemParams='$SystemParams' -FunctionParams='$FunctionParams'"
    $connection_params = ConvertFrom-Json2 $SystemParams

    if ($Class -eq '') {

        if ($GetMeta) {
            #
            # Get all tables and views in database
            #

            Open-ProgressDBConnection $SystemParams

            #
            # Output list of supported operations per table/view (named Class)
            #
            for ($i = 0; $i -lt 21; $i++)
            {
                if($connection_params."table_$($i)_name".length -gt 0)
                {
                    @(
                        [ordered]@{
                            Class = $connection_params."table_$($i)_name"
                            Operation = 'Read'
                            'Source type' = 'Query'
                            'Primary key' = ''
                            'Supported operations' = 'R'
                            'Query' = $connection_params."table_$($i)_query"
                        }
                    )
                    }
            }

        }
        else {
            # Purposely no-operation.
        }

    }
    else {

        if ($GetMeta) {
            #
            # Get meta data
            #

            Open-ProgressDBConnection $SystemParams

            @()

        }
        else {
            #
            # Execute function
            #

            Open-ProgressDBConnection $SystemParams

            for ($i = 0; $i -lt 21; $i++)
            {
                if($connection_params."table_$($i)_name" -eq $class)
                {
                    $class_query = ($connection_params."table_$($i)_query" -split "`n" | ForEach-Object { $_.Trim() } | Where-Object { $_ -ne '' }) -join ' '
                    break
                }
            }

            $column_query = $class_query.replace("SELECT ","SELECT TOP 5 ")
        
            $columns = Fill-SqlInfoCache -Query $column_query
        
            $Global:ColumnsInfoCache[$Class] = @{
                primary_keys = @($columns | Where-Object { $_.is_primary_key } | ForEach-Object { $_.name })
                identity_col = @($columns | Where-Object { $_.is_identity    } | ForEach-Object { $_.name })[0]
            }

            $primary_keys = $Global:ColumnsInfoCache[$Class].primary_keys
            $identity_col = $Global:ColumnsInfoCache[$Class].identity_col

            $function_params = ConvertFrom-Json2 $FunctionParams

            # Replace $null by [System.DBNull]::Value
            $keys_with_null_value = @()
            foreach ($key in $function_params.Keys) { if ($function_params[$key] -eq $null) { $keys_with_null_value += $key } }
            foreach ($key in $keys_with_null_value) { $function_params[$key] = [System.DBNull]::Value }
            
            $sql_command = New-ProgressDBCommand $class_query
            Invoke-ProgressDBCommand $sql_command
            Dispose-ProgressDBCommand $sql_command

        }

    }

    Log info "Done"
}


#
# Helper functions
#

function Fill-SqlInfoCache {
    param (
        [switch] $Force,
        [string] $Query,
        [string] $Class
    )

    # Refresh cache
    $sql_command = New-ProgressDBCommand $Query
    $result = (Invoke-ProgressDBCommand $sql_command) | Get-Member -MemberType Properties | Select-Object Name
    
    Dispose-ProgressDBCommand $sql_command

    $objects = New-Object System.Collections.ArrayList
    $object = @{}
    # Process in one pass
    foreach ($row in $result) {
            $object = @{
                full_name = $Class
                type      = 'Query'
                columns   = New-Object System.Collections.ArrayList
            }

        $object.columns.Add(@{
            name           = $row.Name
            is_primary_key = $false
            is_identity    = $false
            is_computed    = $false
            is_nullable    = $true
        }) | Out-Null
    }

    if ($object.full_name -ne $null) {
        $objects.Add($object) | Out-Null
    }
    @($objects)
}

function New-ProgressDBCommand {
    param (
        [string] $CommandText
    )

    $sql_command = New-Object System.Data.Odbc.OdbcCommand($CommandText, $Global:ProgressDBConnection)
	$sql_command.CommandTimeout = 1800
    return $sql_command
}


function Dispose-ProgressDBCommand {
    param (
        [System.Data.Odbc.OdbcCommand] $SqlCommand
    )

    $SqlCommand.Dispose()
}

function Invoke-ProgressDBCommand {
    param (
        [System.Data.Odbc.OdbcCommand] $SqlCommand
    )

    function Invoke-ProgressDBCommand-ExecuteReader {
        param (
            [System.Data.Odbc.OdbcCommand] $SqlCommand
        )
        $data_reader = $SqlCommand.ExecuteReader()
        $column_names = @($data_reader.GetSchemaTable().ColumnName)

        if ($column_names) {

            $hash_table = [ordered]@{}

            foreach ($column_name in $column_names) {
                $hash_table[$column_name] = ""
            }

#           $obj = [PSCustomObject]$hash_table
            $obj = New-Object -TypeName PSObject -Property $hash_table

            # Read data
            while ($data_reader.Read()) {
                foreach ($column_name in $column_names) {
                    $obj.$column_name = if ($data_reader[$column_name] -is [System.DBNull]) { $null } else { $data_reader[$column_name] }
                }

                # Output data
                $obj
            }

        }

        $data_reader.Close()

    }

    try {
        Invoke-ProgressDBCommand-ExecuteReader $SqlCommand
    }
    catch {
        Log error "Failed: $_"
        Write-Error $_
    }

    Log debug "Done"

}

function Open-ProgressDBConnection {
    param (
        [string] $ConnectionParams
    )

    $connection_params = ConvertFrom-Json2 $ConnectionParams

    $connection_string =  "DRIVER={Progress OpenEdge $($connection_params.driver_version) driver};HOST=$($connection_params.host_name);PORT=$($connection_params.port);DB=$($connection_params.database);UID=$($connection_params.user);PWD=$($connection_params.password);DIL=$($connection_params.isolation_mode);AS=$($connection_params.array_size)"
    
    if($connection_params.enableETWT) { $connectionString += "ETWT=1;" }
    if($connection_params.enableUWCT) { $connectionString += "UWCT=1;" }
    if($connection_params.enableKA) { $connectionString += "KA=1;" }
    LOG info $connection_string
    
    if ($Global:ProgressDBConnection -and $connection_string -ne $Global:ProgressDBConnectionString) {
        Log info "ProgressDBConnection connection parameters changed"
        Close-ProgressDBConnection
    }

    if ($Global:ProgressDBConnection -and $Global:ProgressDBConnection.State -ne 'Open') {
        Log warn "ProgressDBConnection State is '$($Global:ProgressDBConnection.State)'"
        Close-ProgressDBConnection
    }

    Log info "Opening ProgressDBConnection '$connection_string'"

    try {
        $connection = (new-object System.Data.Odbc.OdbcConnection);
        $connection.connectionstring = $connection_string
        $connection.ConnectionTimeout = 3600
		$connection.open();

        $Global:ProgressDBConnection       = $connection
        $Global:ProgressDBConnectionString = $connection_string

        $Global:ColumnsInfoCache = @{}
    }
    catch {
        Log warn "Failed: $_"
        #Write-Error $_
    }

    Log info "Done"
    
}


function Close-ProgressDBConnection {
    if ($Global:ProgressDBConnection) {
        Log info "Closing ProgressDBConnection"

        try {
            $Global:ProgressDBConnection.Close()
            $Global:ProgressDBConnection = $null
        }
        catch {
            # Purposely ignoring errors
        }

        Log info "Done"
    }
}
