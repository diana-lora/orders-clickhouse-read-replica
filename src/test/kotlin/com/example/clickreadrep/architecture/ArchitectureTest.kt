package com.example.clickreadrep.architecture

import com.lemonappdev.konsist.api.Konsist
import com.lemonappdev.konsist.api.ext.list.withPackage
import com.lemonappdev.konsist.api.verify.assertFalse
import com.lemonappdev.konsist.api.verify.assertTrue
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

const val BASE_PACKAGE = "com.example.clickreadrep"
const val COMMONS_PACKAGE = "com.example.clickreadrep.commons"

internal class OrderArchitectureTest {
    companion object {
        const val ORDER_PACKAGE = "$BASE_PACKAGE.order"
        const val APPLICATION_PACKAGE = "$ORDER_PACKAGE.application"
        const val INFRASTRUCTURE_PACKAGE = "$ORDER_PACKAGE.infra"
        const val DOMAIN_PACKAGE = "$ORDER_PACKAGE.domain"
    }

    @Test
    fun `subpackages structure should contain domain, infra and application packages`() {
        val orderPackage =
            Konsist
                .scopeFromProject()
                .files
                .withPackage(listOf("$ORDER_PACKAGE.."))
        assertThat(orderPackage.isNotEmpty()).isTrue()

        val orderDomainPackage =
            Konsist
                .scopeFromProject()
                .files
                .withPackage(listOf("$DOMAIN_PACKAGE.."))
        assertThat(orderDomainPackage.isNotEmpty()).isTrue()

        val orderInfraPackage =
            Konsist
                .scopeFromProject()
                .files
                .withPackage(listOf("$INFRASTRUCTURE_PACKAGE.."))
        assertThat(orderInfraPackage.isNotEmpty()).isTrue()

        val orderApplicationPackage =
            Konsist
                .scopeFromProject()
                .files
                .withPackage(listOf("$APPLICATION_PACKAGE.."))
        assertThat(orderApplicationPackage.isNotEmpty()).isTrue()
    }

    @Test
    fun `domain package should not depend on application or infrastructure packages`() {
        Konsist
            .scopeFromProject()
            .files
            .withPackage(listOf("$DOMAIN_PACKAGE.."))
            .assertFalse { file ->
                file.imports.any { import ->
                    import.name.contains("$APPLICATION_PACKAGE") ||
                        import.name.contains("$INFRASTRUCTURE_PACKAGE")
                }
            }
    }

    @Test
    fun `application package should not depend on infrastructure package`() {
        Konsist
            .scopeFromProject()
            .files
            .withPackage("$APPLICATION_PACKAGE..")
            .assertFalse { file ->
                file.imports.any { import ->
                    import.name.contains("$INFRASTRUCTURE_PACKAGE")
                }
            }
    }

    @Test
    fun `infrastructure package should not depend on external feature packages`() {
        // Production code only: tests in this package legitimately use shared test utilities and configuration
        Konsist
            .scopeFromProduction()
            .files
            .withPackage("$INFRASTRUCTURE_PACKAGE..")
            .assertTrue { file ->
                // Filters imports to verify they don't leak into other feature boundaries like 'history'
                val externalFeatureImports =
                    file.imports.filter { import ->
                        val isInternalProjectImport = import.name.contains(BASE_PACKAGE)
                        val targetsOrderPackage = import.name.contains(ORDER_PACKAGE)
                        val targetsCommonsPackage = import.name.contains(COMMONS_PACKAGE)

                        // It's a violation if it is a project import but fails to target both allowed scopes
                        isInternalProjectImport && !targetsOrderPackage && !targetsCommonsPackage
                    }
                externalFeatureImports.isEmpty()
            }
    }
}
