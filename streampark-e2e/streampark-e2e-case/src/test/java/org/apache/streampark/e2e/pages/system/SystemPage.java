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

package org.apache.streampark.e2e.pages.system;

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
public final class SystemPage extends NavBarPage implements NavBarItem {

    @FindBy(className = "menu-item-system_user")
    public WebElement menuUserManagement;

    @FindBy(className = "menu-item-system_token")
    public WebElement menuTokenManagement;

    @FindBy(className = "menu-item-system_role")
    public WebElement menuRoleManagement;

    @FindBy(className = "menu-item-system_team")
    public WebElement menuTeamManagement;

    @FindBy(className = "menu-item-system_member")
    public WebElement menuMemberManagement;

    public SystemPage(RemoteWebDriver driver) {
        super(driver);
    }

    @SneakyThrows
    public <T extends SystemPage.Tab> T goToTab(Class<T> tab) {
        if (tab == UserManagementPage.class) {
            new WebDriverWait(driver, Constants.DEFAULT_WEBDRIVER_WAIT_DURATION)
                .until(ExpectedConditions.elementToBeClickable(menuUserManagement));
            Thread.sleep(Constants.DEFAULT_SLEEP_MILLISECONDS);
            menuUserManagement.click();
            return tab.cast(new UserManagementPage(driver));
        }

        if (tab == TeamManagementPage.class) {
            new WebDriverWait(driver, Constants.DEFAULT_WEBDRIVER_WAIT_DURATION)
                .until(ExpectedConditions.elementToBeClickable(menuTeamManagement));
            Thread.sleep(Constants.DEFAULT_SLEEP_MILLISECONDS);
            menuTeamManagement.click();
            return tab.cast(new TeamManagementPage(driver));
        }

        if (tab == RoleManagementPage.class) {
            new WebDriverWait(driver, Constants.DEFAULT_WEBDRIVER_WAIT_DURATION)
                .until(ExpectedConditions.elementToBeClickable(menuRoleManagement));
            Thread.sleep(Constants.DEFAULT_SLEEP_MILLISECONDS);
            menuRoleManagement.click();
            return tab.cast(new RoleManagementPage(driver));
        }
        if (tab == TokenManagementPage.class) {
            new WebDriverWait(driver, Constants.DEFAULT_WEBDRIVER_WAIT_DURATION)
                .until(ExpectedConditions.elementToBeClickable(menuTokenManagement));
            Thread.sleep(Constants.DEFAULT_SLEEP_MILLISECONDS);
            menuTokenManagement.click();
            return tab.cast(new TokenManagementPage(driver));
        }

        if (tab == MemberManagementPage.class) {
            new WebDriverWait(driver, Constants.DEFAULT_WEBDRIVER_WAIT_DURATION)
                .until(ExpectedConditions.elementToBeClickable(menuMemberManagement));
            Thread.sleep(Constants.DEFAULT_SLEEP_MILLISECONDS);
            menuMemberManagement.click();
            return tab.cast(new MemberManagementPage(driver));
        }

        throw new UnsupportedOperationException("Unknown tab: " + tab.getName());
    }

    public interface Tab {
    }
}
