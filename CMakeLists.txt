cmake_minimum_required(VERSION 3.15)
project(FreeRTOS_Win32_Demo C)

set(CMAKE_C_STANDARD 99)

# --- Kernel kaynak kodu yolu ---
set(KERNEL_DIR ${CMAKE_SOURCE_DIR}/../../Source)

# --- FreeRTOS kernel dosyaları ---
set(FREERTOS_SOURCES
    ${KERNEL_DIR}/tasks.c
    ${KERNEL_DIR}/queue.c
    ${KERNEL_DIR}/list.c
    ${KERNEL_DIR}/timers.c
    ${KERNEL_DIR}/event_groups.c
    ${KERNEL_DIR}/portable/MemMang/heap_4.c
    ${KERNEL_DIR}/portable/MSVC-MingW/port.c
)

# --- Demo (uygulama) dosyaları ---
set(DEMO_SOURCES
    main.c
    cJSON/cJSON.c
)

add_executable(freertos_demo
    ${FREERTOS_SOURCES}
    ${DEMO_SOURCES}
)

target_include_directories(freertos_demo PRIVATE
    ${CMAKE_SOURCE_DIR}
    ${CMAKE_SOURCE_DIR}/cJSON
    ${KERNEL_DIR}/include
    ${KERNEL_DIR}/portable/MSVC-MingW
)


# Windows'a ozel: zamanlayici (timer) ve soket (network) kutuphaneleri gerekli
target_link_libraries(freertos_demo winmm ws2_32)