rootProject.name = "stock-reading"

pluginManagement {
    includeBuild("build-logic")
}

dependencyResolutionManagement {
    repositories {
        mavenCentral()
    }
}

include(":services:collector")
include(":services:market")
include(":services:news")
include(":services:notifier")
