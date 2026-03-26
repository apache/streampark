/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.streampark.e2e.pages.resource;

import org.apache.streampark.e2e.pages.common.Constants;
import org.apache.streampark.e2e.pages.common.NavBarPage;
import org.apache.streampark.e2e.pages.common.NavBarPage.NavBarItem;

import lombok.Getter;
import lombok.SneakyThrows;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.remote.RemoteWebDriver;
import org.openqa.selenium.support.FindBy;
import org.openqa.selenium.support.ui.ExpectedConditions;
import org.openqa.selenium.support.ui.WebDriverWait;

@Getter
public final class ResourcePage extends NavBarPage implements NavBarItem {

    @FindBy(className = "menu-item-resource_variable")
    public WebElement menuVariables;

    @FindBy(className = "menu-item-resource_project")
    public WebElement menuProjects;

    @FindBy(className = "menu-item-resource_upload")
    public WebElement menuUploads;

    public ResourcePage(RemoteWebDriver driver) {
        super(driver);
    }

    @SneakyThrows
    public <T extends ResourcePage.Tab> T goToTab(Class<T> tab) {
        if (tab == VariablesPage.class) {
            new WebDriverWait(driver, Constants.DEFAULT_WEBDRIVER_WAIT_DURATION)
                .until(ExpectedConditions.elementToBeClickable(menuVariables));
            Thread.sleep(Constants.DEFAULT_SLEEP_MILLISECONDS);
            menuVariables.click();
            return tab.cast(new VariablesPage(driver));
        }

        if (tab == ProjectsPage.class) {
            new WebDriverWait(driver, Constants.DEFAULT_WEBDRIVER_WAIT_DURATION)
                .until(ExpectedConditions.elementToBeClickable(menuProjects));
            Thread.sleep(Constants.DEFAULT_SLEEP_MILLISECONDS);
            menuProjects.click();
            return tab.cast(new ProjectsPage(driver));
        }

        if (tab == UploadsPage.class) {
            new WebDriverWait(driver, Constants.DEFAULT_WEBDRIVER_WAIT_DURATION)
                .until(ExpectedConditions.elementToBeClickable(menuUploads));
            Thread.sleep(Constants.DEFAULT_SLEEP_MILLISECONDS);
            menuUploads.click();
            return tab.cast(new UploadsPage(driver));
        }

        throw new UnsupportedOperationException("Unknown tab: " + tab.getName());
    }

    public interface Tab {
    }
}
