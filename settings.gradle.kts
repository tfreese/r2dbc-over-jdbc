pluginManagement {
    repositories {
        mavenLocal()
        mavenCentral()
        gradlePluginPortal()
    }

    val versionMyJavaConventionPlugin = providers.gradleProperty("version_myJavaConventionPlugin")
    val versionSpringDependencyManagementPlugin = providers.gradleProperty("version_springDependencyManagementPlugin")

    plugins {
        id("de.freese.gradle.conventions").version(versionMyJavaConventionPlugin).apply(false)
        id("io.spring.dependency-management").version(versionSpringDependencyManagementPlugin).apply(false)
    }
}

// Without rootProject.name the Name of the Projekt-Directory is used.
rootProject.name = "r2dbc-over-jdbc"

dependencyResolutionManagement {
    repositoriesMode.set(RepositoriesMode.FAIL_ON_PROJECT_REPOS)

    repositories {
        mavenLocal()
        mavenCentral()
    }
}
