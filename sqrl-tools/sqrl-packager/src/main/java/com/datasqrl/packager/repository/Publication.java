package com.datasqrl.packager.repository;

import com.datasqrl.config.PackageConfiguration;
import com.datasqrl.config.PackageConfigurationImpl;
import com.google.common.base.Strings;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.NonNull;

@Getter
@NoArgsConstructor
@AllArgsConstructor
public class Publication extends PackageConfigurationImpl {

    String uniqueId;
    String file;
    String hash;
    String authorId;
    String submissionTime;

    public Publication(@NonNull PackageConfiguration pkg, @NonNull String uniqueId, @NonNull String file,
        @NonNull String hash, @NonNull String authorId, @NonNull String submissionTime) {
        super(pkg.getName(), pkg.getVersion(), pkg.getVariant(), pkg.getLatest(), orEmpty(pkg.getType()),
            orEmpty(pkg.getLicense()),
            orEmpty(pkg.getRepository()), orEmpty(pkg.getHomepage()), orEmpty(pkg.getDocumentation()),
            orEmpty(pkg.getReadme()), orEmpty(pkg.getDescription()), pkg.getKeywords());
        pkg.checkInitialized();
        this.uniqueId = uniqueId;
        this.file = file;
        this.hash = hash;
        this.authorId = authorId;
        this.submissionTime = submissionTime;
    }

    private static String orEmpty(String input) {
        if (Strings.isNullOrEmpty(input)) return "";
        else return input;
    }

}
