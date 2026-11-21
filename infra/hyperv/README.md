# infra/hyperv: скрипты VM

PowerShell для Hyper-V на ПК стенда (SRS 6.1, 6.4). Запускает пользователь, агент только пишет код.

| Файл | Назначение |
|---|---|
| `New-StandVMs.ps1` | создаёт VM и расширяет стенд; идемпотентен |
| `Remove-StandVMs.ps1` | удаляет VM из файла параметров |
| `StandVM.psm1` | разбор и проверка параметров, сравнение с фактическим состоянием |
| `stand.minimal.json.example` | минимальный стенд (SRS 6.4): `talos-cp-1`, `talos-w-1` |
| `stand.full.json.example` | полный стенд (таблица SRS 6.1): `talos-cp-1`, `talos-w-1…3`, `lb-1`, `lb-2` |
| `Invoke-Checks.ps1`, `StandVM.Tests.ps1`, `PSScriptAnalyzerSettings.psd1` | проверки без Hyper-V |

## Файл параметров

Скопировать пример в `stand.json` (в git не попадает) и поправить пути, имя адаптера и ISO.

| Поле | Смысл |
|---|---|
| `switchName`, `netAdapterName` | External Virtual Switch и Ethernet-адаптер, на котором он создаётся |
| `vmPath`, `vhdPath` | каталоги конфигураций VM и VHDX |
| `isoPath` | общий ISO (образ Talos из Image Factory), скачивается вручную |
| `vms[].name`, `vcpu`, `ramGB` | имя, число vCPU, RAM в ГБ (допускается дробное: `7.5`, `0.5`) |
| `vms[].disks` | размеры дисков в ГБ; первый — системный, остальные — под данные |
| `vms[].iso` | `true` — общий `isoPath`, строка — свой ISO (Debian для `lb-*`), `false` или отсутствует — без DVD |

## Что делает New-StandVMs.ps1

- Создаёт External vSwitch, если его нет (на момент создания связь хоста кратко прерывается). Существующий switch не меняется.
- Для отсутствующей VM: второе поколение, статическая память (Dynamic Memory выключена), Secure Boot выключен,
  автозапуск при старте Windows, динамические VHDX, DVD с ISO. VM не запускается.
- Существующая VM сравнивается с файлом. Расхождения выводятся предупреждениями. Исправимые (vCPU, RAM,
  недостающий диск, Secure Boot, автозапуск) применяются только с `-Reconcile` и только к выключенной VM.
  Неисправимые (размер диска, поколение) дают код выхода 1. VM, которых нет в файле, не затрагиваются.
- Расширение стенда: запуск с `stand.full.json.example` после минимального. Минимальный `talos-w-1` (6 vCPU, 14 ГБ)
  отличается от полного (3 vCPU, 7,5 ГБ): без `-Reconcile` это только предупреждение.
- MAC-адреса динамические. В конце скрипт выводит таблицу «VM / MAC» для DHCP-резервирования и патчей Talos.
  Статические IP задаются в конфигурации Talos и при установке Debian, не в Hyper-V.

## Запуск (PowerShell от администратора)

```powershell
Copy-Item stand.minimal.json.example stand.json   # поправить значения
.\New-StandVMs.ps1 -ConfigPath .\stand.json -WhatIf
.\New-StandVMs.ps1 -ConfigPath .\stand.json
Start-VM talos-cp-1, talos-w-1
```

Удаление (спрашивает подтверждение для каждой VM; VHDX удаляются только с `-RemoveDisks`):

```powershell
.\Remove-StandVMs.ps1 -ConfigPath .\stand.json -Name talos-w-1 -RemoveDisks
```

## Порядок загрузки

Новая VM загружается с диска, затем с DVD. Пока диск пуст, стартует ISO Talos. После установки Talos на диск
вынуть ISO или выставить загрузку с диска:

```powershell
Set-VMFirmware -VMName talos-w-1 -FirstBootDevice (Get-VMHardDiskDrive -VMName talos-w-1 | Select-Object -First 1)
Get-VMDvdDrive -VMName talos-w-1 | Set-VMDvdDrive -Path $null
```

## Проверка

Автоматическая, без Hyper-V (нужен `pwsh`, модули PSScriptAnalyzer и Pester 5; версии закреплены в
`.github/workflows/pr.yml`):

```sh
make psa          # PSScriptAnalyzer
make infra-test   # Pester: разбор файлов, примеры по SRS 6.1 и 6.4, сравнение состояния (идемпотентность)
```

Без `pwsh` цели пропускаются. Реальное создание VM CI не проверяет. Ручная проверка на стенде:

```powershell
Get-VM | Format-Table Name, Generation, ProcessorCount, MemoryStartup, DynamicMemoryEnabled, AutomaticStartAction
Get-VM | ForEach-Object { "$($_.Name) SecureBoot=$((Get-VMFirmware $_).SecureBoot)" }
.\New-StandVMs.ps1 -ConfigPath .\stand.json    # повторный запуск: без предупреждений и изменений
```
