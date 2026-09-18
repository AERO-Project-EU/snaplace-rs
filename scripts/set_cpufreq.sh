#!/bin/sh

set -eu

F="${1:-2400000}"   # kHz
CPUFREQ=/sys/devices/system/cpu/cpufreq
INTEL_PSTATE=/sys/devices/system/cpu/intel_pstate

write_sysfs() {
    path="$1"
    value="$2"

    if ! printf '%s\n' "$value" > "$path"; then
        echo "error: failed to write '$value' to $path" >&2
        exit 1
    fi
}

require_file() {
    path="$1"
    if [ ! -e "$path" ]; then
        echo "error: missing required sysfs file: $path" >&2
        exit 1
    fi
}

if [ "$(id -u)" -ne 0 ]; then
    echo "error: run as root, e.g. sudo $0" >&2
    exit 1
fi

if [ ! -d "$CPUFREQ" ]; then
    echo "error: cpufreq sysfs directory does not exist: $CPUFREQ" >&2
    exit 1
fi

echo "target frequency: ${F} kHz"

# Disable turbo / boost:
# - intel_pstate active/passive, including scaling_driver=intel_cpufreq:
#    -> /sys/devices/system/cpu/intel_pstate/no_turbo
# - acpi-cpufreq:
#    -> /sys/devices/system/cpu/cpufreq/boost
disabled_boost=0
if [ -e "$INTEL_PSTATE/no_turbo" ]; then
    echo "disabling turbo via $INTEL_PSTATE/no_turbo"
    write_sysfs "$INTEL_PSTATE/no_turbo" 1
    disabled_boost=1
fi
if [ -e "$CPUFREQ/boost" ]; then
    echo "disabling boost via $CPUFREQ/boost"
    write_sysfs "$CPUFREQ/boost" 0
    disabled_boost=1
fi
if [ "$disabled_boost" -eq 0 ]; then
    echo "error: found neither intel_pstate/no_turbo nor cpufreq/boost; cannot guarantee turbo is disabled" >&2
    exit 1
fi

found_policy=0

for p in "$CPUFREQ"/policy*; do
    [ -d "$p" ] || continue
    found_policy=1

    require_file "$p/scaling_driver"
    require_file "$p/scaling_available_governors"
    require_file "$p/scaling_governor"
    require_file "$p/scaling_min_freq"
    require_file "$p/scaling_max_freq"
    require_file "$p/cpuinfo_min_freq"
    require_file "$p/cpuinfo_max_freq"

    affected="$(cat "$p/affected_cpus" 2>/dev/null || true)"
    if [ -z "$affected" ]; then
        echo "skipping inactive policy: $p"
        continue
    fi

    driver="$(cat "$p/scaling_driver")"
    governors="$(cat "$p/scaling_available_governors")"
    cpuinfo_min="$(cat "$p/cpuinfo_min_freq")"
    cpuinfo_max="$(cat "$p/cpuinfo_max_freq")"

    echo
    echo "configuring $p"
    echo "  driver:          $driver"
    echo "  affected CPUs:   $affected"
    echo "  governors:       $governors"
    echo "  cpuinfo range:   ${cpuinfo_min}-${cpuinfo_max} kHz"

    if [ "$F" -lt "$cpuinfo_min" ] || [ "$F" -gt "$cpuinfo_max" ]; then
        echo "error: target ${F} kHz outside cpuinfo range for $p" >&2
        exit 1
    fi

    if [ -r "$p/scaling_available_frequencies" ]; then
        freqs="$(cat "$p/scaling_available_frequencies")"
        echo "  available freqs: $freqs"

        if ! printf '%s\n' "$freqs" | grep -qw "$F"; then
            echo "error: target ${F} kHz is not listed in $p/scaling_available_frequencies" >&2
            exit 1
        fi
    fi

    if ! printf '%s\n' "$governors" | grep -qw performance; then
        echo "error: performance governor unavailable for $p" >&2
        exit 1
    fi

    # Use the common path that works for both:
    #   acpi-cpufreq  -> generic performance governor + min=max
    #   intel_cpufreq -> generic performance governor + min=max
    write_sysfs "$p/scaling_governor" performance

    cur_min="$(cat "$p/scaling_min_freq")"
    cur_max="$(cat "$p/scaling_max_freq")"

    # Avoid invalid intermediate states.
    #
    # If current min is above target, lower min first.
    # If current max is below target, raise max first.
    if [ "$cur_min" -gt "$F" ]; then
        write_sysfs "$p/scaling_min_freq" "$F"
    fi

    if [ "$cur_max" -lt "$F" ]; then
        write_sysfs "$p/scaling_max_freq" "$F"
    fi

    # Final clamp.
    write_sysfs "$p/scaling_max_freq" "$F"
    write_sysfs "$p/scaling_min_freq" "$F"

    echo "  final governor:  $(cat "$p/scaling_governor")"
    echo "  final min:       $(cat "$p/scaling_min_freq")"
    echo "  final max:       $(cat "$p/scaling_max_freq")"
done

if [ "$found_policy" -eq 0 ]; then
    echo "error: no cpufreq policy directories found" >&2
    exit 1
fi

echo
echo "done."
echo "verify with:"
echo "  cpupower frequency-info"
echo "  turbostat --quiet --interval 1 --show CPU,Core,Busy%,Bzy_MHz,Avg_MHz,PkgWatt"

