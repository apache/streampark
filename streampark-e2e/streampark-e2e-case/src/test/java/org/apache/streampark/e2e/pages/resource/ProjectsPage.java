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

import lombok.Getter;
import lombok.SneakyThrows;
import org.openqa.selenium.By;
import org.openqa.selenium.Keys;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.remote.RemoteWebDriver;
import org.openqa.selenium.support.FindBy;
import org.openqa.selenium.support.FindBys;
import org.openqa.selenium.support.PageFactory;
import org.openqa.selenium.support.ui.ExpectedConditions;
import org.openqa.selenium.support.ui.WebDriverWait;

import java.util.List;

@Getter
public class ProjectsPage extends NavBarPage implements ResourcePage.Tab {

    @FindBy(id = "e2e-project-create-btn")
    public WebElement buttonCreateProject;

    @FindBy(className = "ant-table-tbody")
    public List<WebElement> projectList;

    @FindBy(className = "e2e-project-build-btn")
    public WebElement buildButton;

    @FindBy(className = "e2e-project-delete-confirm")
    public WebElement deleteConfirmButton;

    @FindBy(className = "e2e-project-build-confirm")
    public WebElement buildConfirmButton;

    public CreateProjectForm createProjectForm;

    public ProjectsPage(RemoteWebDriver driver) {
        super(driver);
        createProjectForm = new CreateProjectForm();
    }

    @SneakyThrows
    public ProjectsPage createProject(String projectName,
                                      String projectUrl,
                                      String projectRefs,
                                      String projectBuildArgument,
                                      String projectDescription) {
        waitForPageLoading();

        new WebDriverWait(driver, Constants.DEFAULT_WEBDRIVER_WAIT_DURATION)
            .until(ExpectedConditions.elementToBeClickable(buttonCreateProject));

        buttonCreateProject.click();

        new WebDriverWait(driver, Constants.DEFAULT_WEBDRIVER_WAIT_DURATION)
            .until(ExpectedConditions.urlContains("/project/add"));

        createProjectForm.inputProjectName.sendKeys(projectName);

        createProjectForm.selectCveDropdown.click();

        new WebDriverWait(driver, Constants.DEFAULT_WEBDRIVER_WAIT_DURATION)
            .until(d -> createProjectForm.selectCve.stream().anyMatch(WebElement::isDisplayed));

        createProjectForm.selectCve.stream()
            .filter(e -> e.getText().equalsIgnoreCase("GitHub/GitLab"))
            .findFirst()
            .orElseThrow(() -> new Exception("Cvs not found"))
            .click();

        createProjectForm.inputProjectUrl.sendKeys(projectUrl);
        createProjectForm.selectBranchDropdown.click();

        new WebDriverWait(driver, Constants.DEFAULT_WEBDRIVER_WAIT_DURATION)
            .until(d -> createProjectForm.selectRefs.stream().anyMatch(WebElement::isDisplayed));
        createProjectForm.selectRefs.stream()
            .filter(e -> e.getText().equalsIgnoreCase(projectRefs))
            .findFirst()
            .orElseThrow(() -> new Exception(String.format("Branch not found: %s", projectRefs)))
            .click();

        createProjectForm.inputBuildArgument.sendKeys(projectBuildArgument);
        createProjectForm.inputDescription.sendKeys(projectDescription);
        createProjectForm.buttonSubmit.click();

        return this;
    }

    @SneakyThrows
    public ProjectsPage editProject(String projectName,
                                    String newProjectName) {
        waitForPageLoading();

        projectList.stream()
            .filter(it -> it.getText().contains(projectName))
            .flatMap(
                it -> it.findElements(
                    By.className("e2e-project-edit-btn"))
                    .stream())
            .filter(WebElement::isDisplayed)
            .findFirst()
            .orElseThrow(() -> new RuntimeException("No edit button in project list"))
            .click();

        new WebDriverWait(driver, Constants.DEFAULT_WEBDRIVER_WAIT_DURATION)
            .until(ExpectedConditions.urlContains("/project/edit"));

        createProjectForm.inputProjectName.sendKeys(Keys.CONTROL + "a");
        createProjectForm.inputProjectName.sendKeys(Keys.BACK_SPACE);
        createProjectForm.inputProjectName.sendKeys(newProjectName);
        createProjectForm.buttonSubmit.click();

        return this;
    }

    @SneakyThrows
    public ProjectsPage buildProject(String projectName) {
        waitForPageLoading();

        projectList.stream()
            .filter(it -> it.getText().contains(projectName))
            .flatMap(
                it -> it.findElements(By.className("e2e-project-build-btn")).stream())
            .filter(WebElement::isDisplayed)
            .findFirst()
            .orElseThrow(() -> new RuntimeException("No build button in project list"))
            .click();

        new WebDriverWait(driver, Constants.DEFAULT_WEBDRIVER_WAIT_DURATION)
            .until(ExpectedConditions.elementToBeClickable(buildConfirmButton));

        buildConfirmButton.click();

        return this;
    }

    @SneakyThrows
    public ProjectsPage deleteProject(String projectName) {
        waitForPageLoading();
        projectList.stream()
            .filter(it -> it.getText().contains(projectName))
            .flatMap(
                it -> it
                    .findElements(By.className("e2e-project-delete-btn"))
                    .stream())
            .filter(WebElement::isDisplayed)
            .findFirst()
            .orElseThrow(() -> new RuntimeException("No delete button in project list"))
            .click();

        new WebDriverWait(driver, Constants.DEFAULT_WEBDRIVER_WAIT_DURATION)
            .until(ExpectedConditions.elementToBeClickable(deleteConfirmButton));

        deleteConfirmButton.click();
        return this;
    }

    private void waitForPageLoading() {
        new WebDriverWait(driver, Constants.DEFAULT_WEBDRIVER_WAIT_DURATION)
            .until(ExpectedConditions.urlContains("/resource/project"));
    }

    @Getter
    private class CreateProjectForm {

        CreateProjectForm() {
            PageFactory.initElements(driver, this);
        }

        @FindBy(id = "form_item_name")
        public WebElement inputProjectName;

        @FindBy(xpath = "//div[contains(@codefield, 'repository')]//div[contains(@class, 'ant-select-selector')]")
        public WebElement selectCveDropdown;

        @FindBys({
                @FindBy(css = "[codefield=repository]"),
                @FindBy(className = "ant-select-item-option-content")
        })
        public List<WebElement> selectCve;

        @FindBy(name = "url")
        public WebElement inputProjectUrl;

        @FindBy(xpath = "//div[contains(@codefield, 'refs')]//div[contains(@class, 'ant-select-selector')]")
        public WebElement selectBranchDropdown;

        @FindBys({
                @FindBy(css = "[codefield=refs]"),
                @FindBy(className = "ant-select-item-option-content")
        })
        public List<WebElement> selectRefs;

        @FindBy(id = "form_item_buildArgs")
        public WebElement inputBuildArgument;

        @FindBy(id = "form_item_description")
        public WebElement inputDescription;

        @FindBy(id = "e2e-project-submit-btn")
        public WebElement buttonSubmit;
    }
}
