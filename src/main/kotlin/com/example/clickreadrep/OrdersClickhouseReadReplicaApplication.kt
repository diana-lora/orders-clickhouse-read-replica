package com.example.clickreadrep

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication

@SpringBootApplication
class OrdersClickhouseReadReplicaApplication

fun main(args: Array<String>) {
    runApplication<OrdersClickhouseReadReplicaApplication>(*args)
}
