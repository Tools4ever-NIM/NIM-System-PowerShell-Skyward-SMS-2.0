$Log_MaskableKeys = @(
    'password',
	'connection_string'
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
                name = 'connection_header'
                type = 'text'
                text = 'Connection'
				tooltip = 'Connection information for the database'
            }
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
                name = 'isolation_mode'
                type = 'textbox'
                label = 'Isolation Mode'
                value = 'READ UNCOMMITTED'
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
                name = 'user'
                type = 'textbox'
                label = 'Username'
                description = 'User account name to access server'
            }
            @{
                name = 'password'
                type = 'textbox'
                password = $true
                label = 'Password'
                description = 'User account password to access server'
            }
			@{
                name = 'query_timeout'
                type = 'textbox'
                label = 'Query Timeout'
                description = 'Time it takes for the query to timeout'
                value = '1800'
            }
			@{
                name = 'connection_timeout'
                type = 'textbox'
                label = 'Connection Timeout'
                description = 'Time it takes for the ODBC Connection to timeout'
                value = '3600'
            }
			@{
                name = 'vpn_header'
                type = 'text'
                text = 'VPN'
				tooltip = 'VPN to access network for database (e.g. ISCorp). Not recommended. Use Site-to-Site VPN instead'
            }
			@{
                name = 'enableVPN'
                type = 'checkbox'
                label = 'Use VPN'
                value = $false
            }
            @{
                name = 'vpnOpenPath'
                type = 'textbox'
                label = 'Open VPN Path'
                description = 'Path to script start connection to vpn'
                value = 'C:\\Tools4ever\\Scripts\\connectSkywardVPN.bat'
				disabled = '!enableVPN'
                hidden = '!enableVPN'
            }
            @{
                name = 'vpnClosePath'
                type = 'textbox'
                label = 'Close VPN Path'
                description = 'Path to script close connection to vpn'
                value = 'C:\\Tools4ever\\Scripts\\disconnectSkywardVPN.bat'
				disabled = '!enableVPN'
                hidden = '!enableVPN'
            }
			@{
                name = 'session_header'
                type = 'text'
                text = 'Session Options'
				tooltip = 'Options for system session'
            }
			@{
                name = 'nr_of_sessions'
                type = 'textbox'
                label = 'Max. number of simultaneous sessions'
                tooltip = ''
                value = 1
            }
            @{
                name = 'sessions_idle_timeout'
                type = 'textbox'
                label = 'Session cleanup idle time (minutes)'
                tooltip = ''
                value = 1
            }
        )
    }

    if ($TestConnection) {
        Open-ProgressDBConnection $ConnectionParams
        
        $query = "SELECT TBL 'Name', 'Table' `"Type`"  FROM sysprogress.SYSTABLES_FULL WHERE TBLTYPE = 'T' ORDER BY TBL"
        $result = (Invoke-ProgressDBCommand $query $ConnectionParams)
		Close-ProgressDBConnection
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

$SqlInfoCache = @{}

function Fill-SqlInfoCache {
    param (
        [switch] $Force,
		[string] $Table,
		[string] $ConnectionParams
    )
	
	if($Table.length -gt 0) {
		$sql_command = "
			SELECT SYSPROGRESS.SYSCOLUMNS.TBL `"full_object_name`",
					'Table' `"object_type`",
					SYSPROGRESS.SYSCOLUMNS.COL `"column_name`",
					CASE WHEN LENGTH(PK.COLNAME) > 0 THEN 1 ELSE 0 END `"is_primary_key`",
					0 `"is_identity`",
					0 `"is_computed`",
					CASE WHEN SYSPROGRESS.SYSCOLUMNS.NULLFLAG = 'Y' THEN 1 ELSE 0 END `"is_nullable`"
			 FROM SYSPROGRESS.SYSCOLUMNS 
			 LEFT JOIN (
				SELECT SYSPROGRESS.`"SYSINDEXES`".COLNAME,SYSPROGRESS.SYSTABLES_FULL.TBL
				from pub.`"_index`"
				JOIN SYSPROGRESS.SYSTABLES_FULL ON SYSPROGRESS.SYSTABLES_FULL.`"PRIME_INDEX`" = pub.`"_index`".ROWID
				JOIN SYSPROGRESS.`"SYSINDEXES`" ON SYSPROGRESS.`"SYSINDEXES`".ID = pub.`"_index`".`"_idx-num`"
				AND SYSPROGRESS.SYSTABLES_FULL.TBLTYPE = 'T'
			 ) PK ON PK.COLNAME = SYSPROGRESS.SYSCOLUMNS.COL AND PK.TBL = SYSPROGRESS.SYSCOLUMNS.TBL
			 WHERE SYSPROGRESS.SYSCOLUMNS.TBL = '$($Table)'
			ORDER BY
				SYSPROGRESS.SYSCOLUMNS.TBL, SYSPROGRESS.SYSCOLUMNS.COL
		"
	} else {
		
		if (!$Force -and $Global:SqlInfoCache.Ts -and ((Get-Date) - $Global:SqlInfoCache.Ts).TotalMilliseconds -le [Int32]3600000) {
			return
		}

		# Refresh cache
		$sql_command = "
			SELECT SYSPROGRESS.SYSCOLUMNS.TBL `"full_object_name`",
					'Table' `"object_type`",
					SYSPROGRESS.SYSCOLUMNS.COL `"column_name`",
					CASE WHEN LENGTH(PK.COLNAME) > 0 THEN 1 ELSE 0 END `"is_primary_key`",
					0 `"is_identity`",
					0 `"is_computed`",
					CASE WHEN SYSPROGRESS.SYSCOLUMNS.NULLFLAG = 'Y' THEN 1 ELSE 0 END `"is_nullable`"
			 FROM SYSPROGRESS.SYSCOLUMNS 
			 LEFT JOIN (
				SELECT SYSPROGRESS.`"SYSINDEXES`".COLNAME,SYSPROGRESS.SYSTABLES_FULL.TBL
				from pub.`"_index`"
				JOIN SYSPROGRESS.SYSTABLES_FULL ON SYSPROGRESS.SYSTABLES_FULL.`"PRIME_INDEX`" = pub.`"_index`".ROWID
				JOIN SYSPROGRESS.`"SYSINDEXES`" ON SYSPROGRESS.`"SYSINDEXES`".ID = pub.`"_index`".`"_idx-num`"
				AND SYSPROGRESS.SYSTABLES_FULL.TBLTYPE = 'T'
			 ) PK ON PK.COLNAME = SYSPROGRESS.SYSCOLUMNS.COL AND PK.TBL = SYSPROGRESS.SYSCOLUMNS.TBL
			ORDER BY
				SYSPROGRESS.SYSCOLUMNS.TBL, SYSPROGRESS.SYSCOLUMNS.COL
		"
	}

    $objects = New-Object System.Collections.ArrayList
    $object = @{}

    # Process in one pass
    Invoke-ProgressDBCommand $sql_command $ConnectionParams | ForEach-Object {
        if ($_.full_object_name -ne $object.full_name) {
            if ($object.full_name -ne $null) {
                $objects.Add($object) | Out-Null
            }

            $object = @{
                full_name = $_.full_object_name
                type      = $_.object_type
                columns   = New-Object System.Collections.ArrayList
            }
        }

        $object.columns.Add(@{
            name           = $_.column_name
            is_primary_key = $_.is_primary_key
            is_identity    = $_.is_identity
            is_computed    = $_.is_computed
            is_nullable    = $_.is_nullable
        }) | Out-Null
    }

    if ($object.full_name -ne $null) {
        $objects.Add($object) | Out-Null
    }

    $Global:SqlInfoCache.Objects = $objects
    $Global:SqlInfoCache.Ts = Get-Date
}

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

    if ($Class -eq '') {

        if ($GetMeta) {
            #
            # Get all tables and views in database
            #
			Open-ProgressDBConnection $SystemParams
            Fill-SqlInfoCache -ConnectionParams $SystemParams
			
            #
            # Output list of supported operations per table/view (named Class)
            #

             @(
                foreach ($object in $Global:SqlInfoCache.Objects) {
                    $primary_keys = $object.columns | Where-Object { $_.is_primary_key } | ForEach-Object { $_.name }

                    if ($object.type -ne 'Table') {
                        # Non-tables only support 'Read'
                        [ordered]@{
                            Class = $object.full_name
                            Operation = 'Read'
                            'Source type' = $object.type
                            'Primary key' = $primary_keys -join ', '
                            'Supported operations' = 'R'
                        }
                    }
                    else {
                        [ordered]@{
                            Class = $object.full_name
                            Operation = 'Create'
                        }

                        [ordered]@{
                            Class = $object.full_name
                            Operation = 'Read'
                            'Source type' = $object.type
                            'Primary key' = $primary_keys -join ', '
                            'Supported operations' = "CR$(if ($primary_keys) { 'UD' } else { '' })"
                        }

                        if ($primary_keys) {
                            # Only supported if primary keys are present
                            [ordered]@{
                                Class = $object.full_name
                                Operation = 'Update'
                            }

                            [ordered]@{
                                Class = $object.full_name
                                Operation = 'Delete'
                            }
                        }
                    }
                }
            )
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
            Fill-SqlInfoCache -Table $Class -ConnectionParams $SystemParams

            $columns = ($Global:SqlInfoCache.Objects | Where-Object { $_.full_name -eq $Class }).columns

            switch ($Operation) {
                'Create' {
                    @{
                        semantics = 'create'
                        parameters = @(
                            $columns | ForEach-Object {
                                @{
                                    name = $_.name;
                                    allowance = if ($_.is_identity -or $_.is_computed) { 'prohibited' } elseif (! $_.is_nullable) { 'mandatory' } else { 'optional' }
                                }
                            }
                        )
                    }
                    break
                }

                'Read' {
                    @(
                        @{
                            name = 'select_distinct'
                            type = 'checkbox'
                            label = 'Distinct Rows'
                            description = 'Apply Distinct to select'
                            value = $false
                        }
                        @{
                            name = 'where_clause'
                            type = 'textbox'
                            label = 'Filter (SQL where-clause)'
                            description = 'Applied SQL where-clause'
                            value = ''
                        }
                        @{
                            name = 'selected_columns'
                            type = 'grid'
                            label = 'Include columns'
                            description = 'Selected columns'
                            table = @{
                                rows = @($columns | ForEach-Object {
                                    @{
                                        name = $_.name
                                        config = @(
                                            if ($_.is_primary_key) { 'Primary key' }
                                            if ($_.is_identity)    { 'Generated' }
                                            if ($_.is_computed)    { 'Computed' }
                                            if ($_.is_nullable)    { 'Nullable' }
                                        ) -join ' | '
                                    }
                                })
                                settings_grid = @{
                                    selection = 'multiple'
                                    key_column = 'name'
                                    checkbox = $true
                                    filter = $true
                                    columns = @(
                                        @{
                                            name = 'name'
                                            display_name = 'Name'
                                        }
                                        @{
                                            name = 'config'
                                            display_name = 'Configuration'
                                        }
                                    )
                                }
                            }
                            value = @($columns | ForEach-Object { $_.name })
                        }
                    )
                    break
                }

                'Update' {
                    @{
                        semantics = 'update'
                        parameters = @(
                            $columns | ForEach-Object {
                                @{
                                    name = $_.name;
                                    allowance = if ($_.is_primary_key) { 'mandatory' } else { 'optional' }
                                }
                            }
                            @{
                                name = '*'
                                allowance = 'prohibited'
                            }
                        )
                    }
                    break
                }

                'Delete' {
                    @{
                        semantics = 'delete'
                        parameters = @(
                            $columns | ForEach-Object {
                                if ($_.is_primary_key) {
                                    @{
                                        name = $_.name
                                        allowance = 'mandatory'
                                    }
                                }
                            }
                            @{
                                name = '*'
                                allowance = 'prohibited'
                            }
                        )
                    }
                    break
                }
            }

        }
        else {
            #
            # Execute function
            #
            Open-ProgressDBConnection $SystemParams

            if (! $Global:ColumnsInfoCache[$Class]) {
                Fill-SqlInfoCache -Table $Class -ConnectionParams $SystemParams

                $columns = ($Global:SqlInfoCache.Objects | Where-Object { $_.full_name -eq $Class }).columns
				
                $Global:ColumnsInfoCache[$Class] = @{
                    primary_keys = @($columns | Where-Object { $_.is_primary_key } | ForEach-Object { $_.name })
                    identity_col = @($columns | Where-Object { $_.is_identity    } | ForEach-Object { $_.name })[0]
                }
				
            }

            $primary_key  = $Global:ColumnsInfoCache[$Class].primary_keys
            $identity_col = $Global:ColumnsInfoCache[$Class].identity_col

            $function_params = ConvertFrom-Json2 $FunctionParams

            $command = $null

            $projection = if ($function_params['selected_columns'].count -eq 0) { '*' } else { @($function_params['selected_columns'] | ForEach-Object { "`"$_`"" }) -join ', ' }

            switch ($Operation) {
                'Create' {
                    $selection = if ($identity_col) {
                                     "[$identity_col] = SCOPE_IDENTITY()"
                                 }
                                 elseif ($primary_key) {
                                     "[$primary_key] = '$($function_params[$primary_key])'"
                                 }
                                 else {
                                     @($function_params.Keys | ForEach-Object { "`"$_`" = '$($function_params[$_])'" }) -join ' AND '
                                 }

                    $command = "INSERT INTO `"PUB`".`"$Class`" ($(@($function_params.Keys | ForEach-Object { '"'+$_+'"' }) -join ', ')) VALUES ($(@($function_params.Keys | ForEach-Object { "$(if ($function_params[$_] -ne $null) { "'$($function_params[$_])'" } else { 'null' })" }) -join ', '));SELECT TOP 1 $projection FROM `"PUB`".`"$Class`" WHERE $selection"
                    break
                }

                'Read' {
                    $selection = if ($function_params['where_clause'].length -eq 0) { '' } else { " WHERE $($function_params['where_clause'])" }

                    $command = "SELECT $projection FROM `"PUB`".`"$Class`"$selection"
                    break
                }

                'Update' {
					$command = "UPDATE `"PUB`".`"$Class`" SET $(@($function_params.Keys | ForEach-Object { if ($_ -ne $primary_key) { '"{0}" = {1}' -f $_,"$(if ($function_params[$_] -ne $null) { "'$($function_params[$_])'" } else { 'null' })" } }) -join ', ') WHERE `"$primary_key`" = '$($function_params[$primary_key])';SELECT `"$primary_key`", $(@($function_params.Keys | ForEach-Object { if ($_ -ne $primary_key) { '"{0}"' -f $_ } }) -join ', ') FROM `"PUB`".`"$Class`" WHERE `"$primary_key`" = '$($function_params[$primary_key])'"
					break
                }

                'Delete' {
                    $command = "DELETE TOP 1 `"PUB`".`"$Class`" WHERE [$primary_key] = '$($function_params[$primary_key])'"
                    break
                }
            }
			
            if ($command) {
                LogIO info ($command -split ' ')[0] -In -Command $command
				
                if ($Operation -eq 'Read') {
                    # Streamed output
                    Invoke-ProgressDBCommand $command $ConnectionParams
                }
                else {
                    # Log output
                    foreach($cmd in ($command -split ';')) {
						$rv = Invoke-ProgressDBCommand ($cmd -replace ';','') $ConnectionParams
						LogIO info ($cmd -split ' ')[0] -Out $rv

						if($cmd.StartsWith('UPDATE') -or $cmd.StartsWith('CREATE')) {
							#skip result
						} else {
							$rv
						}
					}
                }
            }

        }

    }
	Close-ProgressDBConnection
    Log info "Done"
}


#
# Helper functions
#

function Invoke-ProgressDBCommand {
    param (
        [string] $Command,
		[string] $ConnectionParams
    )

    function Invoke-ProgressDBCommand-ExecuteReader {
        param (
            [string] $Command,
			[string] $Timeout
        )
		$sql_command  = New-Object System.Data.Odbc.OdbcCommand($Command, $Global:ProgressDBConnection)
		$sql_command.CommandTimeout = $Timeout
        $data_adapter = New-Object System.Data.Odbc.OdbcDataAdapter($sql_command)
        $data_table   = New-Object System.Data.DataTable
        $data_adapter.Fill($data_table) | Out-Null

        # Output data
        $data_table.Rows | Select $data_table.Columns.ColumnName

        $data_table.Dispose()
        $data_adapter.Dispose()
        $sql_command.Dispose()
    }
    
	$connection_params = ConvertFrom-Json2 $ConnectionParams
	
	$Command = ($Command -split "`n" | ForEach-Object { $_.Trim() } | Where-Object { $_ -ne '' }) -join ' '

    try {
        Invoke-ProgressDBCommand-ExecuteReader $Command $connection_params.query_timeout
    }
    catch {
        Log error "Failed: $_"
        Write-Error $_
    }
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
	
    $Global:enableVPN = $connection_params.enableVPN
    $Global:vpnOpenPath = $connection_params.vpnOpenPath
    $Global:vpnClosePath = $connection_params.vpnClosePath

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
        #Force close any connections before connecting
        Close-ProgressDBVPN 
        Open-ProgressDBVPN
        
        $connection = (new-object System.Data.Odbc.OdbcConnection);
        $connection.connectionstring = $connection_string
		$connection.ConnectionTimeout = 3600
        $connection.open();

        $Global:ProgressDBConnection       = $connection
        $Global:ProgressDBConnectionString = $connection_string

        $Global:ColumnsInfoCache = @{}
    }
    catch {
        Log error "Failed: $_"
        Write-Error $_
    }

    Log info "Done"
    
}


function Close-ProgressDBConnection {
    if ($Global:ProgressDBConnection) {
        Log info "Closing ProgressDBConnection"

        try {
            
            $Global:ProgressDBConnection.Close()
            $Global:ProgressDBConnection = $null
            Close-ProgressDBVPN
        }
        catch {
            # Purposely ignoring errors
        }

        

        Log info "Done"
    }
}

function Open-ProgressDBVPN {
    if ($Global:enableVPN -eq $true)
    {
        Log info "Opening vpn..."        

        $vpnOutput = Get-ProcessOutput -FileName $Global:vpnOpenPath
        Log info $vpnOutput.StandardOutput

        if($vpnOutput.StandardError -ne $null)
        {
            Log error $vpnOutput.StandardError
        }
        Log info "Connected to vpn."
    }
}


function Close-ProgressDBVPN {
    if ($Global:enableVPN -eq $true)
    {
        Log info "Closing vpn..."

        $vpnOutput = Get-ProcessOutput -FileName $Global:vpnClosePath
        Log info $vpnOutput.StandardOutput

        if($vpnOutput.StandardError -ne $null)
        {
            Log error $vpnOutput.StandardError
        }

        Log info "Closed vpn."
    }
}

function Get-ProcessOutput
{
    Param (
                [Parameter(Mandatory=$true)]$FileName,
                $Args
    )
    
    $process = New-Object System.Diagnostics.Process
    $process.StartInfo.UseShellExecute = $false
    $process.StartInfo.RedirectStandardOutput = $true
    $process.StartInfo.RedirectStandardError = $true
    $process.StartInfo.FileName = $FileName
    if($Args) { $process.StartInfo.Arguments = $Args }
    $out = $process.Start()
    $process.WaitForExit(30000)
    $StandardError = $process.StandardError.ReadToEnd()
    $StandardOutput = $process.StandardOutput.ReadToEnd()
    
    $output = New-Object PSObject
    $output | Add-Member -type NoteProperty -name StandardOutput -Value $StandardOutput
    $output | Add-Member -type NoteProperty -name StandardError -Value $StandardError
    return $output
}
