#!/bin/bash
set -euo pipefail
IFS=$'\n\t'

# Ловушка для отладки при критических ошибках
trap 'echo "[FATAL] Скрипт прерван на строке $LINENO. Проверьте логи." >&2; exit 1' ERR

# Имена файлов индексов
SRC_INDEX_NAME=".index_source.tsv"
BKP_INDEX_NAME=".index_backup.tsv"

show_help() {
    echo "Использование: $0 [КЛЮЧ] [КАТАЛОГ_ИСТОЧНИКА] [КАТАЛОГ_БЭКАПА]"
    echo "Ключи управления индексами (ручные):"
    echo "  --init-source-index /disk1 /disk2       Полный пересчет index источника с нуля"
    echo "  --update-source-index /disk1 /disk2     Быстрое обновление index источника"
    echo "  --init-backup-index /disk1 /disk2       Полный пересчет index приемника с нуля"
    echo "  --update-backup-index /disk1 /disk2     Быстрое обновление index приемника"
    echo "Ключи синхронизации:"
    echo "  --dry-run /disk1 /disk2                 Только АНАЛИЗ и ОТЧЕТ (без изменения диска)"
    echo "  --sync /disk1 /disk2                    Обновление индексов + Отчет + Реальный Бэкап"
    exit 1
}

if [ "$#" -lt 3 ]; then show_help; fi

MODE="$1"
DIR1=$(realpath "$2" 2>/dev/null) || { echo "Ошибка: невалидный путь источника"; exit 1; }
DIR2=$(realpath "$3" 2>/dev/null) || { echo "Ошибка: невалидный путь бэкапа"; exit 1; }

# Жёсткая проверка наличия xxhsum
if ! command -v xxhsum &> /dev/null; then
    echo "КРИТИЧЕСКАЯ ОШИБКА: Утилита 'xxhsum' не найдена. Установите её: sudo apt install xxhash" >&2
    exit 3
fi

get_deleted_dirname() {
    local bkp_dir="$1"
    local base
    base=$(basename "$bkp_dir")
    if [ "$base" == "/" ] || [ -z "$base" ]; then echo ".DELETED_"; else echo ".DELETED_$base"; fi
}
DEL_DIR_NAME=$(get_deleted_dirname "$DIR2")
DEL_PATH="$DIR2/$DEL_DIR_NAME"

bytes_to_mb() {
    echo "$1" | awk '{printf "%.2f", $1 / 1048576}'
}

# --- ФУНКЦИЯ ДИАГНОСТИКИ (СРАВНЕНИЯ) ДЛЯ ИНДЕКСОВ ---
diagnose_difference() {
    local src_idx="$DIR1/$SRC_INDEX_NAME"
    local bkp_idx="$DIR2/$BKP_INDEX_NAME"

    if [ ! -f "$src_idx" ] || [ ! -f "$bkp_idx" ]; then
        echo "[Диагностика] Второй индекс отсутствует. Сравнение невозможно."
        return 0
    fi

    declare -A s_files b_files
    local total_s=0 total_b=0 matched=0 changed=0

    while IFS=$'\t' read -r type mtime hash rel_path size dir_id; do
        if [ "$type" == "F" ]; then
            s_files["$rel_path"]="$hash"
            total_s=$((total_s + 1))
        fi
    done < "$src_idx"

    while IFS=$'\t' read -r type mtime hash rel_path size dir_id; do
        if [ "$type" == "F" ]; then
            b_files["$rel_path"]="$hash"
            total_b=$((total_b + 1))
        fi
    done < "$bkp_idx"

    for s_path in "${!s_files[@]}"; do
        if [ -n "${b_files[$s_path]+x}" ]; then
            if [ "${s_files[$s_path]}" == "${b_files[$s_path]}" ]; then
                matched=$((matched + 1))
            else
                changed=$((changed + 1))
            fi
        fi
    done

    echo "=== Диагностика баланса дисков ==="
    echo "Файлов на источнике: $total_s | Живых файлов в бэкапе: $total_b"
    echo "Полностью совпадают: $matched | Изменились по хэшу: $changed"
    echo "=================================="
}

# --- ФУНКЦИЯ ИНДЕКСАЦИИ С ПРОГРЕССОМ ---
build_index() {
    local target_dir="$1"
    local index_file="$2"
    local is_update="$3"
    local is_backup_dir="$4"
    
    if [ ! -d "$target_dir" ]; then
        echo "Ошибка: Каталог '$target_dir' не существует." >&2
        return 1
    fi

    # Расчёт базового размера для прогресса
    local total_bytes=0
    if [ "$is_update" == "true" ] && [ -f "$index_file" ]; then
        total_bytes=$(awk -F'\t' '($1=="F" || $1=="TF") {sum+=$5} END {print sum+0}' "$index_file")
    else
        total_bytes=$(du -sb "$target_dir" 2>/dev/null | cut -f1)
    fi
    [ "$total_bytes" -eq 0 ] && total_bytes=1

    declare -A old_mtime old_hash old_size
    if [ "$is_update" == "true" ] && [ -f "$index_file" ]; then
        while IFS=$'\t' read -r type mtime hash rel_path size dir_id; do
            dir_id=$(echo "$dir_id" | tr -d '\r')
            if [[ "$type" == "F" || "$type" == "TF" ]]; then
                local key="$type:$rel_path"
                old_mtime["$key"]="$mtime"
                old_hash["$key"]="$hash"
                old_size["$key"]="$size"
            fi
        done < "$index_file"
    fi

    local tmp_index
    tmp_index=$(mktemp)
    declare -A dir_ids dir_parents
    local dir_counter=1

    # 1. Каталоги
    while IFS= read -r -d '' dir; do
        [ "$dir" == "$target_dir" ] && continue
        
        local rel_d="${dir#$target_dir/}"
        local type="D"
        
        if [ "$is_backup_dir" == "true" ] && [[ "$rel_d" == "$DEL_DIR_NAME" || "$rel_d" == "$DEL_DIR_NAME"/* ]]; then
            type="TD"
            [ "$rel_d" == "$DEL_DIR_NAME" ] && continue
            rel_d="${rel_d#$DEL_DIR_NAME/}"
        fi

        dir_ids["$type:$dir"]=$dir_counter
        local parent_dir
        parent_dir=$(dirname "$dir")
        
        local p_type="$type"
        if [ "$parent_dir" == "$target_dir" ] || [ "$parent_dir" == "$DEL_PATH" ]; then
            dir_parents["$type:$dir"]=1
        else
            dir_parents["$type:$dir"]="${dir_ids[$p_type:$parent_dir]}"
        fi
        
        printf "%s\t%s\t%s\t%s\t%s\t%s\n" "$type" "$(stat -c %Y "$dir")" "$(basename "$dir")" "$rel_d" "$dir_counter" "${dir_parents[$type:$dir]}" >> "$tmp_index"
        dir_counter=$((dir_counter + 1))
    done < <(find "$target_dir" -type d -print0 | sort -z)

    local files_processed=0
    local hash_calculated=0
    local processed_bytes=0

    # 2. Файлы
    while IFS= read -r -d '' file; do
        local rel_path="${file#$target_dir/}"
        local type="F"

        if [ "$is_backup_dir" == "true" ] && [[ "$rel_path" == "$DEL_DIR_NAME" || "$rel_path" == "$DEL_DIR_NAME"/* ]]; then
            type="TF"
            rel_path="${rel_path#$DEL_DIR_NAME/}"
        fi

        local current_mtime current_size f_dir f_dir_id
        current_mtime=$(stat -c %Y "$file")
        current_size=$(stat -c %s "$file")
        f_dir=$(dirname "$file")
        f_dir_id=1


        
        if [ "$f_dir" != "$target_dir" ] && [ "$f_dir" != "$DEL_PATH" ]; then
            f_dir_id="${dir_ids[$type:$f_dir]}"
        fi

        local final_hash=""
        local idx_key="$type:$rel_path"
        local stored_mtime="${old_mtime[$idx_key]:-}"
        local stored_size="${old_size[$idx_key]:-}"
        
        if [ "$is_update" == "true" ] && [ -n "$stored_mtime" ] && [ "$stored_mtime" -eq "$current_mtime" ] && \
           [ -n "$stored_size" ] && [ "$stored_size" -eq "$current_size" ]; then
            final_hash="${old_hash[$idx_key]}"
        else
            final_hash=$(xxhsum -H128 "$file" | awk '{print $1}')
            hash_calculated=$((hash_calculated + 1))
        fi

        printf "%s\t%s\t%s\t%s\t%s\t%s\n" "$type" "$current_mtime" "$final_hash" "$rel_path" "$current_size" "$f_dir_id" >> "$tmp_index"
        
        files_processed=$((files_processed + 1))
        processed_bytes=$((processed_bytes + current_size))

        # Динамический прогресс-бар
        local pct=$(( processed_bytes * 100 / total_bytes ))
        local display_bytes
        if (( processed_bytes >= 1073741824 )); then
            display_bytes="$(( processed_bytes / 1073741824 )) ГБ"
        else
            display_bytes="$(( processed_bytes / 1048576 )) МБ"
        fi
        
        if [ "$is_update" == "true" ]; then
            printf "\r[Индексация] Файлов: %d | %s | %d%% | Пересчёт хэшей: %d          " "$files_processed" "$display_bytes" "$pct" "$hash_calculated"
        else
            printf "\r[Индексация] Файлов: %d | %s | %d%% | Обработано          " "$files_processed" "$display_bytes" "$pct"
        fi
    done < <(find "$target_dir" -path "$DEL_PATH" -prune -o -type f -not -name "$SRC_INDEX_NAME" -not -name "$BKP_INDEX_NAME" -print0)


    echo ""
    mv "$tmp_index" "$index_file"
    echo "Индекс успешно сохранен в $index_file"
    diagnose_difference
}

# --- ФУНКЦИЯ СИНХРОНИЗАЦИИ ---
sync_data() {
    local dry_run_mode="$1"

    echo "=== Шаг 0: Автоматическое обновление индексов по mtime и размеру ==="
    echo "Проверка и быстрое обновление index источника..."
    build_index "$DIR1" "$DIR1/$SRC_INDEX_NAME" "true" "false"
    
    echo "Проверка и быстрое обновление index бэкапа..."
    build_index "$DIR2" "$DIR2/$BKP_INDEX_NAME" "true" "true"

    local src_idx="$DIR1/$SRC_INDEX_NAME"
    local bkp_idx="$DIR2/$BKP_INDEX_NAME"

    declare -A src_f_hash src_f_size src_f_exists src_hash_to_path src_d_exists
    declare -A bkp_f_hash bkp_f_size bkp_f_exists bkp_hash_to_path bkp_d_exists
    declare -A trash_f_hash trash_f_size trash_f_exists trash_hash_to_path

    # Чтение источника
    while IFS=$'\t' read -r type arg2 hash rel_path size dir_id; do
        hash=$(echo "$hash" | tr -d '\r')
        if [ "$type" == "F" ]; then
            src_f_hash["$rel_path"]="$hash"
            src_f_size["$rel_path"]="$size"
            src_f_exists["$rel_path"]=1
            src_hash_to_path["$hash"]="$rel_path"
        elif [ "$type" == "D" ]; then
            src_d_exists["$rel_path"]=1
        fi
    done < "$src_idx"

    local trash_total_size=0
    # Чтение бэкапа (ВОССТАНОВЛЕНА СТРОКА bkp_hash_to_path)
    while IFS=$'\t' read -r type arg2 hash rel_path size dir_id; do
        hash=$(echo "$hash" | tr -d '\r')
        if [ "$type" == "F" ]; then
            bkp_f_hash["$rel_path"]="$hash"
            bkp_f_size["$rel_path"]="$size"
            bkp_f_exists["$rel_path"]=1
            bkp_hash_to_path["$hash"]="$rel_path"
        elif [ "$type" == "D" ]; then
            bkp_d_exists["$rel_path"]=1
        elif [ "$type" == "TF" ]; then
            trash_f_hash["$rel_path"]="$hash"
            trash_f_size["$rel_path"]="$size"
            trash_f_exists["$rel_path"]=1
            trash_hash_to_path["$hash"]="$rel_path"
            trash_total_size=$((trash_total_size + ${size:-0}))
        fi
    done < "$bkp_idx"

    local count_add=0 count_mv=0 count_upd=0 count_del=0 count_restore=0
    local count_d_add=0 count_d_del=0
    local required_space=0

    declare -A plan_action plan_target_name

    # Анализ файлов источника
    for s_rel in "${!src_f_exists[@]}"; do
        local s_hash="${src_f_hash[$s_rel]}"
        local s_size="${src_f_size[$s_rel]}"

        if [ -n "${bkp_f_exists[$s_rel]+x}" ]; then
            if [ "$s_hash" != "${bkp_f_hash[$s_rel]}" ]; then
                count_upd=$((count_upd + 1))
                required_space=$((required_space + s_size))
                plan_action["$s_rel"]="UPDATE"
            fi
        else
            if [ -n "${bkp_hash_to_path[$s_hash]+x}" ]; then
                count_mv=$((count_mv + 1))
                plan_action["$s_rel"]="MOVE_LIVE"
                plan_target_name["$s_rel"]="${bkp_hash_to_path[$s_hash]}"
            elif [ -n "${trash_hash_to_path[$s_hash]+x}" ]; then
                count_restore=$((count_restore + 1))
                plan_action["$s_rel"]="RESTORE_TRASH"
                plan_target_name["$s_rel"]="${trash_hash_to_path[$s_hash]}"
            else
                count_add=$((count_add + 1))
                required_space=$((required_space + s_size))
                plan_action["$s_rel"]="ADD"
            fi
        fi
    done

    # Анализ файлов бэкапа на удаление
    for b_rel in "${!bkp_f_exists[@]}"; do
        local b_hash="${bkp_f_hash[$b_rel]}"
        local mapped_src_path="${src_hash_to_path[$b_hash]:-}"
        local mapped_action=""
        [ -n "$mapped_src_path" ] && mapped_action="${plan_action[$mapped_src_path]:-}"

        if [ -z "${src_f_exists[$b_rel]+x}" ] && [ "$mapped_action" != "MOVE_LIVE" ]; then
            count_del=$((count_del + 1))
        fi
    done

    # Анализ каталогов
    for s_d in "${!src_d_exists[@]}"; do
        [ -z "${bkp_d_exists[$s_d]+x}" ] && count_d_add=$((count_d_add + 1))
    done
    for b_d in "${!bkp_d_exists[@]}"; do
        [ -z "${src_d_exists[$b_d]+x}" ] && count_d_del=$((count_d_del + 1))
    done

    # Получение доступного места (надежный способ)
    local available_space
    available_space=$(df -B1 "$DIR2" | awk 'NR==2 {print $4}')
    available_space=${available_space//[^0-9]/}

    echo "=== ПРЕДВАРИТЕЛЬНЫЙ АНАЛИЗ СИНХРОНИЗАЦИИ ==="
    echo "Новых файлов будет добавлено:        $count_add"
    echo "Файлов восстановится из корзины:     $count_restore"
    echo "Файлов переместится внутри бэкапа:   $count_mv"
    echo "Файлов обновится (новая версия):     $count_upd"
    echo "Файлов уйдёт в корзину (удаление):   $count_del"
    echo "--------------------------------------------"
    echo "Новых каталогов будет создано:       $count_d_add"
    echo "Устаревших каталогов будет удалено:  $count_d_del"
    echo "--------------------------------------------"
    echo "Текущий размер корзины на бэкапе:    $(bytes_to_mb $trash_total_size) МБ"
    echo "Требуется места для копирования:     $(bytes_to_mb $required_space) МБ"
    echo "Доступно на диске бэкапа:            $(bytes_to_mb $available_space) МБ"
    echo "============================================"

    if [ "$required_space" -gt "$available_space" ]; then
        echo "КРИТИЧЕСКАЯ ОШИБКА: Недостаточно свободного места. Синхронизация отменена." >&2
        exit 2
    fi

    if [ "$dry_run_mode" == "true" ]; then
        echo "[Dry Run] Режим имитации активен. Все дисковые операции пропущены."
        return 0
    fi

    # Шаг 1: Создание каталогов
    echo "=== Шаг 1: Создание новых каталогов ==="
    for s_d in "${!src_d_exists[@]}"; do
        if [ -z "${bkp_d_exists[$s_d]+x}" ]; then
            echo "[Создание каталога] $s_d"
            mkdir -p "$DIR2/$s_d"
        fi
    done

    # Шаг 2: Перемещения и восстановления
    echo "=== Шаг 2: Локальные перемещения и восстановления ==="
    for s_rel in "${!plan_action[@]}"; do
        local act="${plan_action[$s_rel]}"
        if [ "$act" == "MOVE_LIVE" ]; then
            local old_b_path="${plan_target_name[$s_rel]}"
            echo "[Перемещение] $old_b_path -> $s_rel"
            mkdir -p "$(dirname "$DIR2/$s_rel")"
            mv "$DIR2/$old_b_path" "$DIR2/$s_rel"
        elif [ "$act" == "RESTORE_TRASH" ]; then
            local trash_old_path="${plan_target_name[$s_rel]}"
            echo "[Восстановление из корзины] $trash_old_path -> $s_rel"
            mkdir -p "$(dirname "$DIR2/$s_rel")"
            mv "$DEL_PATH/$trash_old_path" "$DIR2/$s_rel"
        fi
    done

    # Шаг 3: Удаление в корзину
    echo "=== Шаг 3: Отправка удаляемых файлов в корзину ==="
    for b_rel in "${!bkp_f_exists[@]}"; do
        if [ -z "${src_f_exists[$b_rel]+x}" ] && [ -f "$DIR2/$b_rel" ]; then
            echo "[Файл в корзину] $b_rel"
            mkdir -p "$(dirname "$DEL_PATH/$b_rel")"
            mv "$DIR2/$b_rel" "$DEL_PATH/$b_rel"
        fi
    done

    # Шаг 4: Копирование новых и измененных (сохраняем атрибуты!)
    echo "=== Шаг 4: Копирование новых и изменившихся файлов ==="
    for s_rel in "${!plan_action[@]}"; do
        local act="${plan_action[$s_rel]}"
        if [ "$act" == "ADD" ] || [ "$act" == "UPDATE" ]; then
            if [ "$act" == "UPDATE" ]; then
                echo "[Обновление версии] $s_rel -> корзина"
                mkdir -p "$(dirname "$DEL_PATH/$s_rel")"
                mv "$DIR2/$s_rel" "$DEL_PATH/$s_rel"
            else
                echo "[Новый файл] $s_rel"
            fi
            mkdir -p "$(dirname "$DIR2/$s_rel")"
            cp -a "$DIR1/$s_rel" "$DIR2/$s_rel"
        fi
    done

    # Шаг 5: Удаление пустых каталогов
    echo "=== Шаг 5: Удаление пустых устаревших каталогов ==="
    find "$DIR2" -mindepth 1 -type d -empty \
        -not -path "$DEL_PATH" -not -path "${DEL_PATH}/*" \
        -not -path "$DIR2/.index_*" \
        -exec rmdir {} + 2>/dev/null || true
    echo "[Каталоги] Очистка завершена."

    # Шаг 6: Финальный пересчет
    echo "=== Шаг 6: Финальный пересчет индекса бэкапа ==="
    build_index "$DIR2" "$DIR2/$BKP_INDEX_NAME" "true" "true"
    echo "Синхронизация успешно завершена."
}

# --- КОРНЕВАЯ МАРШРУТИЗАЦИЯ КЛЮЧЕЙ ---
case "$MODE" in
    --init-source-index)   build_index "$DIR1" "$DIR1/$SRC_INDEX_NAME" "false" "false" ;;
    --update-source-index) build_index "$DIR1" "$DIR1/$SRC_INDEX_NAME" "true" "false" ;;
    --init-backup-index)   build_index "$DIR2" "$DIR2/$BKP_INDEX_NAME" "false" "true" ;; 
    --update-backup-index) build_index "$DIR2" "$DIR2/$BKP_INDEX_NAME" "true" "true" ;;  
    --dry-run)             sync_data "true" ;;
    --sync)                sync_data "false" ;;
    *)                     show_help ;;
esac
