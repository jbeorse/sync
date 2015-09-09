# sync

This project is __*actively maintained*__

It is part of the ODK 2.0 Android tools suite.

ODK Sync is a program that allows you to sync data collected by the ODK 2.0 tools with an ODK Aggregate instance.

Instructions on how to use Sync can be found [here](https://opendatakit.org/use/2_0_tools/sync-2-0-rev126/).

The developer [wiki](https://github.com/opendatakit/opendatakit/wiki) (including release notes) and
[issues tracker](https://github.com/opendatakit/opendatakit/issues) are located under
the [**opendatakit**](https://github.com/opendatakit/opendatakit) project.

The Google group for software engineering questions is: [opendatakit-developers@](https://groups.google.com/forum/#!forum/opendatakit-developers)

## Setting up your environment

General instructions for setting up an ODK 2.0 environment can be found at our [DevEnv Setup wiki page](https://github.com/opendatakit/opendatakit/wiki/DevEnv-Setup)

Install [Android Studio](http://developer.android.com/tools/studio/index.html) and the [SDK](http://developer.android.com/sdk/index.html#Other).

This project depends on ODK's [androidlibrary](https://github.com/opendatakit/androidlibrary) and [androidcommon](https://github.com/opendatakit/androidcommon) projects; their binaries will be downloaded automatically fom our maven repository during the build phase. If you wish to modify them yourself, you must clone them into the same parent directory as sync. You directory stucture should resemble the following:

        |-- odk

            |-- androidcommon

            |-- androidlibrary

            |-- sync


  * Note that this only applies if you are modifying the library projects. If you use the maven dependencies (the default option), the projects will not show up in your directory. 
    
ODK [Core](https://github.com/opendatakit/core) __MUST__ be installed on your device, whether by installing the APK or by cloning the project and deploying it. ODK [Survey](https://github.com/opendatakit/survey) and ODK [Tables](https://github.com/opendatakit/tables) also integrate well with ODK Sync; Sync does not have much functionality without them.

Now you should be ready to build.

## Building the project

Open the Sync project in Android Studio. Select `Build->Make Project` to build the app.

## Running

Be sure to install ODK Core onto your device before attempting to run Sync.

## Source tree information
Quick description of the content in the root folder:

    |-- sync_app     -- Source tree for Java components

        |-- src

            |-- main

                |-- res     -- Source tree for Android resources

                |-- java

                    |-- org

                        |-- opendatakit

                            |-- sync

                                |-- android     -- The most relevant Java code lives here
                                
            |-- androidTest    -- Source tree for Android implementation tests
